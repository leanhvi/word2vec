
import json

def order_vec(arr):
    for i in range(len(arr)):
        arr[i] += 2 * i
    return arr

def easy_hash(arr):
    code = [ 1 if i > 0 else 0 for i in arr ]
    for i in range(len(code)):
        code[i] += 2 * i
    return code

def to_json(item):
    import json
    txt = json.dumps(item)
    return txt

def put_to_es(df, es_path):
    rdd = df.rdd.map(lambda row: {'id': row[0], 'word': row[1], 'order_vec': row[2], 'order_hash': row[3]})
    es_rdd = rdd.map(lambda a: ('key', to_json(a) ))

    conf = {
        "es.nodes": "localhost"
        ,"es.port": "9200"
        ,"es.resource": es_path
        ,"es.mapping.id": "id"
        ,"es.input.json": "true"
        ,"es.output.json": "true"
        # ,"es.write.operation": "upsert"
    }

    es_rdd.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=conf
    )