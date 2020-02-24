from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 fact_query="",
                 dim_query="",
                 *args, **kwargs):
        
        '''
        initialises the DataQualityOperator, this is an operator, which 
        checks the number of records between fact and dimensions tables.
        If the unique records between two provided tables are not matching, 
        a value error is thrown.
        
        :redshift_conn_id - Airflow conection for Postgres or Redshft 
            connection
        :table - relevant table name as string
        :fact_query - SQL statement for counting relevant fact records
        _dim_query - SQL statement for counting relevant fact records
        '''

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.fact_query=fact_query
        self.dim_query=dim_query

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        fact_records = redshift_hook.get_records(self.fact_query)
        dim_records = redshift_hook.get_records(self.dim_query.\
                                                format(self.table))
        
        if fact_records != dim_records:
            raise ValueError(f"Fact and dim records mismatch for {self.table}")
            
        self.log.info(f"Data Qualiyt Check for {self.table} succeeded!")