import os
from StringIO import StringIO
from fabric.api import env, lcd, local


#################
# Configuration #
#################

# Spark
env.jar_class_name = 'org.atchai.test.SmallData'
env.jar_path = 'spark/target/scala-2.10/smalldata_2.10-1.0.jar'
env.jar_dependencies = (
    'spark/lib_managed/jars/com.github.scopt/scopt_2.10/scopt_2.10-3.3.0.jar',
    'spark/lib_managed/jars/org.mongodb/mongo-java-driver/mongo-java-driver-2.12.3.jar',
    'spark/lib_managed/jars/org.mongodb.mongo-hadoop/mongo-hadoop-core/mongo-hadoop-core-1.3.2.jar',
    'spark/lib_managed/jars/org.elasticsearch/elasticsearch-spark_2.10/elasticsearch-spark_2.10-2.1.0.Beta4.jar',
)

env.spark_home_local = '/usr/local/opt/apache-spark'
env.spark_home_remote = '/root/spark'
env.spark_ec2_home_remote = '/root/spark-ec2'

env.remote_work_dir = '/root/smalldata'

env.cluster_region = 'eu-west-1'
env.cluster_zone = 'eu-west-1b'
env.cluster_placement_group = 'smalldata-1'

env.cluster_instance = 'c3.2xlarge'
env.cluster_driver_mem = '8g'
env.cluster_executor_mem = '8g'
env.cluster_timeout = 600 # 10 minutes
env.cluster_akka_frame_size = 1024 # 1GB

env.spark_serializer = 'org.apache.spark.serializer.KryoSerializer'

env.es_http_timeout = 600
env.es_http_retries = 100
env.es_index_autocreate = 'true'
env.es_index_refresh = 'false'
env.es_batch_retries = 100
env.es_batch_retry_wait = 600
env.es_batch_entries = 100000
env.es_batch_bytes = 300000000

env.es_content = 'users'

# CPP
env.src_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cpp')
env.build_path = os.path.join(env.src_path, 'build')
env.exec_path = os.path.join(env.build_path, 'smalldata')


#########
# Tasks #
#########

def spark_build():
    """ Build JAR """
    with lcd('spark'):
        local('sbt package')


def spark_run(
        num_cores = 4,
        mem_size = '4g',
        mongo_url = 'mongodb://127.0.0.1:27017',
        mongo_content = 'data.comments',
        file_users = 'data/users.txt',
        es_url = 'http://127.0.0.1:9200',
        es_index = 'smalldata-spark',
        num_terms = 10
    ):
    """ Run process locally """

    # Build JAR
    spark_build()

    # Delete existing ES index
    local(' '.join(('curl', '-XDELETE', '%s/%s' % (es_url, es_index))))

    # Run job locally using "spark-submit"
    run_cmd = (
        os.path.join(env.spark_home_local, 'bin/spark-submit'),
        '--master', 'local[%s]' % num_cores,
        '--driver-memory', mem_size,
        '--conf', 'es.http.timeout=%d' % env.es_http_timeout,
        '--conf', 'es.http.retries=%d' % env.es_http_retries,
        '--conf', 'es.index.auto.create=%s' % env.es_index_autocreate,
        '--conf', 'es.batch.write.refresh=%s' % env.es_index_refresh,
        '--conf', 'es.batch.write.retry.count=%d' % env.es_batch_retries,
        '--conf', 'es.batch.write.retry.wait=%d' % env.es_batch_retry_wait,
        '--conf', 'es.batch.size.entries=%d' % env.es_batch_entries,
        '--conf', 'es.batch.size.bytes=%d' % env.es_batch_bytes,
        '--conf', 'spark.serializer=%s' % env.spark_serializer,
        '--class', env.jar_class_name,
        '--jars', ','.join(env.jar_dependencies),
        env.jar_path,
        mongo_url,
        mongo_content,
        file_users,
        es_url,
        es_index,
        env.es_content,
        str(num_terms),
    )
    local(' '.join(run_cmd))


def cpp_build(build_type='Release'):
    """ Build """
    if not os.path.isdir(env.build_path):
        os.makedirs(env.build_path)
    with lcd(env.build_path):
        local('cmake -DCMAKE_BUILD_TYPE=%s %s' % (build_type, env.src_path))
        local('make VERBOSE=1')


def cpp_run(
        file_users = 'data/users.txt',
        file_comments = 'data/comments.bson',
        es_url = 'http://127.0.0.1:9200',
        es_index = 'smalldata-cpp',
    ):
    """ Build and run """
    cpp_build()

    local(' '.join((
        env.exec_path,
        file_users,
        file_comments,
        es_index,
        env.es_content,
        '>',
        'users_bulk.json'
    )))

    # Delete existing ES index
    local(' '.join(('curl', '-XDELETE', '%s/%s' % (es_url, es_index))))

    # Bulk import
    local(' '.join((
        'curl',
        '-XPOST',
        '%s/_bulk' % es_url,
        '--data-binary',
        '@users_bulk.json',
    )))

    # Remove temp file
    local('rm users_bulk.json')
