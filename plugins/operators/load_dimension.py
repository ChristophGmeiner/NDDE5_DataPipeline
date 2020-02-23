from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_creds="",
                 createsql="",
                 insertsql="",
                 table="",
                 *args, **kwargs):
        
        '''
        initialises the LoadDimensionOperator, this is an operator, which 
        loads data from the database stage tables into the indicated
        dimension tables
        
        :redshift_conn_id - Airflow conection for Postgres or Redshft 
            connection
        :aws_creds - Credentials stored in Airflow connections for accessing 
            the S3 bucket
        :createsql - String indicating the SQL statement for creating the 
            relevant table
        :insertsql - String indicating the SQL statement for inserting data  
            into the relevant table
        :table - relevant table name as string
        '''

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
      
        self.redshift_conn_id = redshift_conn_id
        self.aws_creds = aws_creds
        self.creastesql = createsql
        self.insertsql = insertsql
        self.table = table
    
    def execute(self, context):
        
        delsql = "TRUNCATE TABLE {}"
        inssql = "INSERT INTO {} {}"
        
        rs_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Creating and / or deleting fact table")
        rs_hook.run(self.createsql)
        rs_hook(delsql.format(self.table))
        
        self.log.info("Inserting...")
        rs_hook.run(inssql.format(self.table, self.insertsql))