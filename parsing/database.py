import sqlite3
import uuid
import datetime
conn = sqlite3.connect('results.db')

FORMAT_BENCHMARKS = ['hash', 'uuid', 'benchmark', 'iodepth', 'mode', 'numjobs', 'op_size', 'vol_size', 'block_devices', 'date']
TYPES_BENCHMARKS = {'hash': 'text primary key' , 'uuid': 'text', 'benchmark': 'text', 'iodepth': 'integer', 'mode': 'text', 'numjobs': 'text',
                    'op_size': 'integer', 'vol_size': 'integer', 'block_devices': 'text', 'date': 'timestamp'}

FORMAT_RESULTS = ['hash', 'uuid', 'iodepth', 'mode', 'op_size', 'vol_size', 'block_devices', 'type', 'key', 'value', 'server']
TYPES_RESULTS = {'hash': 'text primary key', 'uuid': 'text', 'iodepth': 'integer', 'mode': 'text',
                 'op_size': 'integer', 'vol_size': 'integer', 'block_devices': 'text', 'type': 'text',
                 'key': 'text', 'value': 'text', 'server': 'text'}


def exists(fields, table):
    c = conn.cursor()
    f = ''
    for k,v in fields.iteritems():
        f += '%s="%s" and ' % (k,v)
    q = "SELECT count(*) from %s where %s" % (table, f[:-4])
    c.execute(q)
    results = c.fetchone()
    if results[0] == 0:
        return True
    return False


def create_db():
    c = conn.cursor()
    q = 'CREATE TABLE if not exists benchmark ('
    values = []
    for key in FORMAT_BENCHMARKS:
        values.append("%s %s" % (key, TYPES_BENCHMARKS[key]))
    q += ', '.join(values)+')'
    c.execute(q)
    conn.commit()

    q = 'CREATE TABLE if not exists results ('
    values = []
    for key in FORMAT_RESULTS:
        values.append("%s %s" % (key, TYPES_RESULTS[key]))
    q += ', '.join(values)+')'
    c.execute(q)
    conn.commit()


def insert_benchmark(values):
    c = conn.cursor()
    q = 'INSERT INTO benchmark ( hash, date, '
    v = 'VALUES ('
    values1 = [str(uuid.uuid1())]
    values1.append(datetime.datetime.now())

    for key,vv in values.iteritems():
        q += '%s, ' % key
        v += '?, '
        values1.append(vv)

    # Remove the last comma from fields names
    q = q[:-2]
    q += ") "

    v += " ?, ?) "

    query = "%s%s" % (q, v)
    c.execute(query, values1)
    conn.commit()


def insert_results(values, params):
    # 'uuid', 'iodepth', 'mode', 'op_size', 'vol_size', 'block_devices', 'type', 'key', 'value'
    c = conn.cursor()
    query = "INSERT INTO results ('uuid', 'iodepth', 'mode', 'op_size', 'vol_size', 'block_devices', 'type', 'key', 'value', 'hash', 'server') "
    query += "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    c.execute(query, values)
    conn.commit()


def get_benchmarks(buuid = None):
    c = conn.cursor()
    if buuid is None:
        query = 'SELECT * FROM benchmark ORDER BY date'
        c.execute(query)
        return c.fetchall()

def get_values(column):
    c = conn.cursor()
    # Careful here, this could lead to an SQL injection but appears necessary
    # since bindings can't be used for column names.
    c.execute('SELECT distinct %s FROM results ORDER BY %s' % (column, column))
    return [item[0] for item in c.fetchall()]


def fetch_table(params):
    c = conn.cursor()
    distincts = {}

    for param in params:
        distincts[param] = get_values(param)

    c.execute('SELECT testname,%s,readbw,writebw FROM results ORDER BY %s,testname' % (','.join(params), ','.join(params)))
    testnames = get_values('testname')

    table = []
    writerow = []
    readrow = []
    for row in c.fetchall():
        # Check to make sure we aren't missing a test
        while row[0] != testnames[len(writerow)]:
             blank = ['%s' % testnames[len(writerow)], '']
             writerow.append(blank)
             readrow.append(blank)
        writerow.append([row[0],row[-1]])
        readrow.append([row[0],row[-2]])
        if len(writerow) == len(testnames):
             pre = []
             for i in xrange(0, len(params)):
                  pre.append([params[i],row[i+1]])
             table.append(pre + [['optype', 'write']] + writerow)
             table.append(pre + [['optype', 'read']] + readrow)
             writerow = []
             readrow = []
    return table
