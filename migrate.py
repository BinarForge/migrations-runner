import psycopg2
import os
import sys
import configparser 

# ---
# Command line migration script runner
# ---

parser = configparser.RawConfigParser()
parser.read('settings.cfg')

env = 'dev'
if len(sys.argv) > 2:
    env = sys.argv[2]

try:
    config = (parser.get(env, 'dbname'), parser.get(env, 'user'), parser.get(env, 'host'), parser.get(env, 'password'))
except Exception as e:
    print('No config found for [{0}] environment!'.format((env)))
    exit()

# Decide where to read the migration status from
# DB / Local File
status_source = parser.get(env, 'status')
if status_source is None or status_source == '':
    status_source = 'file'


def run_migration(id):
    try:
        connect_str = "dbname='{0}' user='{1}' host='{2}' password='{3}'".format(*config)
        
        conn = psycopg2.connect(connect_str)
        cursor = conn.cursor()

        try:
            with open('./migrations/'+id+'/up.sql') as file:
                data = file.read()
                cursor.execute(data)
                conn.commit()
                cursor.close()

                print('*migration ['+id+'] completed!')
                return True
        except Exception as e:
            print('*migration ['+id+'] FAILED!')
            print(e)
            cursor.close()
            return False
        
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)
        return False

def find_lastest_migration():
    for dirname, dirnames, filenames in os.walk('./migrations'):
        idx = len(dirnames)-1
        if idx < 0:
            return '000'
        else:
            return dirnames[idx]

def migrate(m_from, m_to):
    if m_to is None:
        m_to = find_lastest_migration()
    
    if m_from == m_to:
        print('Database is up to date!')
        return m_to

    print('Environment is: ['+env+']')
    print('Migration status source is: ['+status_source+']')
    print('Running migrations from ['+m_from+'] to ['+m_to+']...')

    last = m_from
    for dirname, dirnames, filenames in os.walk('./migrations'):
        for dirname in dirnames:
            if int(dirname) > int(m_from) and (m_to is None or int(dirname) <= int(m_to)):
                if run_migration(dirname):
                    last = dirname

    return last

# ---
# The actual program starts here
# ---

migration_start = '000'
migration_end = None
if len(sys.argv) > 1:
    if os.path.isdir('./migrations/'+sys.argv[1]):
        migration_end = sys.argv[1]
    else:
        print('Migration ['+sys.argv[1]+'] not found!')

last_migration = None
try:

    if status_source == 'file':
        with open('./status.txt') as file:
            status = file.read()
            if status != '' and os.path.isdir('./migrations/'+status):
                migration_start = status

            last_migration = migrate(migration_start, migration_end)
    elif status_source == 'db':
        connect_str = "dbname='{0}' user='{1}' host='{2}' password='{3}'".format(*config)
        
        conn = psycopg2.connect(connect_str)
        cursor = conn.cursor()
        migration_start = '000'

        try:
            cursor.execute('SELECT COALESCE((SELECT version FROM migration_status LIMIT 1), 0)')
            migration_start = str(cursor.fetchone()[0]).zfill(3)
        except Exception as e:
            conn.rollback()
            cursor.execute('CREATE TABLE IF NOT EXISTS migration_status ( version SMALLINT DEFAULT 0 NOT NULL )')
            cursor.execute('INSERT INTO migration_status VALUES(0)')
            conn.commit()

        cursor.close()
        last_migration = migrate(migration_start, migration_end)
    
except Exception as e:
    last_migration = migrate(migration_start, None)

if status_source == 'file':
    f = open('./status.txt', 'w')
    f.write(last_migration)
    f.close()
elif status_source == 'db':
    connect_str = "dbname='{0}' user='{1}' host='{2}' password='{3}'".format(*config)

    conn = psycopg2.connect(connect_str)
    cursor = conn.cursor()

    cursor.execute('UPDATE migration_status SET version = \''+last_migration+'\'')
    conn.commit()
    cursor.close()

# ---
# The actual program ends here
# ---