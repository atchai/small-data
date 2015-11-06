#include <iostream>
#include <fstream>
#include <cmath>
#include <queue>
#include <regex>
#include <bson.h>
#include "Log.h"
#include "Terms.h"
#include "DistinctiveTerms.h"

const char *Terms::KEY_USER = "user_id";
const char *Terms::KEY_CONTENT = "body";

const char *Terms::PREFIX_HTTP = "http://";
const char *Terms::PREFIX_HTTPS = "https://";

const std::vector<std::string> Terms::STOP_WORDS = {
    "com", "http", "twitter", "pic", "me", "de", "you", "ly", "que", "www", "my", "la", "en",
    "via", "bit", "el", "your", "rt", "so", "one", "im", "se", "new", "instagram", "all", "a",
    "have", "do", "es", "like", "https", "co", "when", "up", "un", "get", "te", "we", "dont",
    "por", "what", "from", "its", "can", "who", "con", "para", "los", "mi", "how", "lo", "now",
    "more", "about", "youtu", "good", "want", "go", "una", "si", "see", "jp", "las", "html",
    "our", "us", "he", "ow", "al", "her", "am", "gl", "goo", "tu", "eu", "has", "vine",
    "youtube", "como", "please", "why", "org", "some", "le", "tweet", "bu", "retweet", "ve",
    "yo", "ne", "net", "you're", "too", "na", "his", "them", "mas", "off", "ask", "a", "above",
    "after", "again", "against", "an", "and", "any", "are", "aren't", "as", "at", "be",
    "because", "been", "before", "being", "below", "between", "both", "but", "by", "cannot",
    "could", "couldn", "did", "didn't", "does", "doesn't", "doing", "don't", "down", "during",
    "each", "few", "for", "further", "had", "hadn't", "hasn't", "haven", "having", "he'd",
    "he's", "here", "here", "hers", "herself", "he'll", "him", "how", "i", "i'm", "i've", "if",
    "in", "into", "is", "isn't", "it", "it's", "itself", "let", "most", "mustn't", "myself", "no",
    "nor", "not", "of", "on", "once", "only", "or", "other", "ought", "ours", "out", "over",
    "own", "same", "shan't", "she", "should", "shouldn't", "such", "than", "that", "the",
    "their", "theirs", "themselves", "then", "there", "there's", "these", "they", "they're",
    "they've", "this", "those", "through", "to", "under", "until", "very", "was", "wasn't", "we",
    "were", "where", "which", "while", "whom", "with", "will", "would", "won't", "wouldn't",
    "yours", "yourself", "yourselves"
};

/**
 * Read in terms from BSON
 */
Terms::Terms(const char *filename, const Users &users)
            : users(users) {

    // Get file size
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    int filesize = in.tellg() / 1024 / 1024;
    in.close();

    // Open file
    bson_reader_t *reader;
    bson_error_t error;
    if (!(reader = bson_reader_new_from_file(filename, &error))) {
        throw std::runtime_error(std::string("Failed to open ") + filename + ": " + error.message);
    }

    // Iterate over BSON objects
    log("Reading and tokenising content");
    bson_iter_t iter;
    const bson_t *b;
    off_t offset = 0;
    while ((b = bson_reader_read(reader, NULL))) {

        if (!bson_iter_init(&iter, b)) {
            throw std::runtime_error("Unable to initialise BSON iterator");
        }

        // Get content
        const char *body = NULL;
        if (bson_iter_find(&iter, KEY_CONTENT) && BSON_ITER_HOLDS_UTF8(&iter)) {
            body = bson_iter_utf8(&iter, NULL);
        }

        // Get user ID
        int user_id = -1;
        if (bson_iter_find(&iter, KEY_USER) && BSON_ITER_HOLDS_INT32(&iter)) {
            user_id = bson_iter_int32(&iter);
        }

        if (body != NULL && user_id != -1) {
            // Tokenise
            tokenise(body, user_id);

            // Increment user's content count
            users_content[user_id]++;
        }

        // Log progress every 10MB
        if (bson_reader_tell(reader) > (offset + (1024 * 1024 * 10))) {
            offset = bson_reader_tell(reader);
            int completed = offset / 1024 / 1024;
            log(
                std::string("Read ") + std::to_string((int)(((float)completed / filesize) * 100)) + "% (" +
                std::to_string(completed) + "MB/" + std::to_string(filesize) + "MB)",
                true
            );
        }

    }

    log("Read total of " + std::to_string(filesize) + "MB from " + filename);

    bson_reader_destroy(reader);
}

/**
 * Tokenise a string
 */
void Terms::tokenise(const char *str, int user_id) {
    do {
        // Find token
        const char *begin = str;
        while(*str != ' ' && *str != '\n' && *str != '\t' && *str) {
            str++;
        }

        // Remove non-alphanumeric, non-@, non-# prefixes
        while(!std::isalnum(*begin) && *begin != '#' && *begin != '@' && begin != str) {
            begin++;
        }

        // Remove non-alphanumeric suffixes
        const char *end = str;
        while((!std::isalnum(*(end-1))) && end != begin) {
            end--;
        }

        // Ignore empty terms or terms that start with "http://" or "https://"
        if (1 > (end - begin) ||
            strncmp(PREFIX_HTTP, begin, strlen(PREFIX_HTTP)) == 0 ||
            strncmp(PREFIX_HTTPS, begin, strlen(PREFIX_HTTPS)) == 0) {
            continue;
        }

        // Convert to lowercase and calculate hash
        auto term = std::string(begin, end);
        unsigned long hash = 5381;
        for(char& c : term) {
            if (c <= 'Z' && c >= 'A') {
                c -= 'Z' - 'z';
            }
            hash = ((hash << 5) + hash) + c;
        }

        // Ignore terms in stop words
        if (std::find(STOP_WORDS.begin(), STOP_WORDS.end(), term) != STOP_WORDS.end()) {
            continue;
        }

        // Map hash to string
        terms.insert(std::make_pair(hash, term));

        // Increment user's count for this term
        users_terms[user_id][hash]++;
    } while (0 != *str++);
}

void Terms::serialise(const char *es_index, const char *es_content) const {

    std::string bulk_header = std::string("{\"index\":{\"_index\":\"") + es_index + "\",\"_type\":\"" + es_content + "\"}}\n";

    // Iterate over users
    for(auto const &user : users_terms) {

        // Get distinctive terms
        DistinctiveTerms distinctive;
        for(auto const &term : user.second) {
            distinctive.add(term);
        }

        // Serialise
        bson_t b;
        bson_init(&b);

        // Add user ID
        BSON_APPEND_INT32(&b, "user_id", user.first);

        // Add username
        BSON_APPEND_UTF8(&b, "username", users.get(user.first).c_str());

        // Add num_comments
        BSON_APPEND_INT32(&b, "num_comments", users_content.at(user.first));

        // Add array for distinctive terms
        bson_t terms_array;
        bson_append_array_begin(&b, "terms", strlen("terms"), &terms_array);

        // Itreate over distinctive terms
        while(!distinctive.empty()) {

            // Add object for distinctive term
            bson_t term_doc;
            bson_append_document_begin(&terms_array, "0", -1, &term_doc);

            // Add term
            BSON_APPEND_UTF8(&term_doc, "key", terms.at(distinctive.top().first).c_str());

            // Add score
            BSON_APPEND_INT32(&term_doc, "score", distinctive.top().second);

            // Close object
            bson_append_document_end(&terms_array, &term_doc);
            distinctive.pop();
        }

        // Close array
        bson_append_array_end(&b, &terms_array);

        // Convert BSON object to JSON string
        char *json_str = bson_as_json(&b, NULL);

        // Output
        std::cout << bulk_header << json_str << "\n";

        // Cleanup
        bson_free(json_str);
        bson_destroy(&b);

    }

}

/**
 * Get count of terms
 */
const int Terms::count() const {
    return terms.size();
}
