from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_creds="",
                 createsql="",
                 insertsql="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
      
        self.redshift_conn_id = redshift_conn_id
        self.aws_creds = aws_creds
        self.creastesql = createsql
        self.insertsql = insertsql
        self.table = table
    
    def execute(self, context):
        #basically the dekete part should be abandoned in productive mode
        delsql = "TRUNCATE TABLE {}"
        inssql = "INSERT INTO {} {}"
        
        rs_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Creating and / or deleting fact table")
        rs_hook.run(self.createsql)
        rs_hook(delsql.format(self.table))
        
        self.log.info("Inserting...")
        rs_hook.run(inssql.format(self.table, self.insertsql))