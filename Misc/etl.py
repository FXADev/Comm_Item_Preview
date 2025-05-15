import os, yaml, logging, datetime, pandas as pd
import psycopg2, psycopg2.extras
from simple_salesforce import Salesforce
from dotenv           import load_dotenv
from helpers          import sql_server_conn, truncate_then_insert
load_dotenv()

def load_cfg():
    with open('config.yml','r',encoding='utf-8') as f:
        return yaml.safe_load(f)

def redshift_conn():
    return psycopg2.connect(
        host     = os.environ['REDSHIFT_HOST'],
        port     = os.environ['REDSHIFT_PORT'],
        dbname   = os.environ['REDSHIFT_DATABASE'],
        user     = os.environ['REDSHIFT_USERNAME'],
        password = os.environ['REDSHIFT_PASSWORD'],
        options  = f"-c search_path={os.environ['REDSHIFT_SCHEMA']}"
    )

def redshift_extract(cfg, mssql_cxn, batch_id, ts):
    with redshift_conn() as pg, pg.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur_pg:
        cur_ms = mssql_cxn.cursor()
        for q in cfg['redshift']['queries']:
            logging.info('Redshift → %s', q['name'])
            cur_pg.execute(q['sql'])
            while True:
                rows = cur_pg.fetchmany(10000)
                if not rows: break
                df = pd.DataFrame(rows, columns=[d.name for d in cur_pg.description])
                df['etl_batch_id'] = batch_id
                df['extracted_at'] = ts
                truncate_then_insert(cur_ms, f"stg_{q['name']}", df)
                mssql_cxn.commit()

def salesforce_extract(cfg, mssql_cxn, batch_id, ts):
    sf_login = cfg['salesforce']['login']
    sf = Salesforce(
        username = os.environ['SALESFORCE_USERNAME'],
        password = os.environ['SALESFORCE_PASSWORD'],
        security_token = os.environ['SALESFORCE_SECURITY_TOKEN'],
        instance_url = os.environ['SALESFORCE_LOGIN_URL']
    )
    cur_ms = mssql_cxn.cursor()
    for obj in cfg['salesforce']['objects']:
        logging.info('SF → %s', obj['object'])
        soql = f"SELECT {', '.join(obj['fields'])} FROM {obj['object']}"
        if obj.get('where'): soql += f" WHERE {obj['where']}"
        recs = sf.bulk.__getattr__(obj['object']).query(soql,
                chunkSize=100000, pk_chunking=True)
        df = pd.DataFrame(recs)
        df['etl_batch_id'] = batch_id
        df['extracted_at'] = ts
        truncate_then_insert(cur_ms, f"stg_sf_{obj['object'].lower()}", df)
        mssql_cxn.commit()

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s')
    cfg  = load_cfg()
    ts   = datetime.datetime.utcnow()
    bid  = ts.strftime('%Y%m%d%H%M%S')
    with sql_server_conn() as mssql:
        redshift_extract(cfg, mssql, bid, ts)
        salesforce_extract(cfg, mssql, bid, ts)
        # run nightly transform; uncomment when proc exists
        # mssql.cursor().execute('EXEC dbo.sp_nightly_commission_preview')
        mssql.commit()
    logging.info('ETL batch %s complete', bid)
