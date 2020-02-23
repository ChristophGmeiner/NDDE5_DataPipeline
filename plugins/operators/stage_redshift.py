from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from create_tables import dropsql

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    copy_sql = """
                    COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    region {} json {}                    
                """
       

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_creds="",
                 createsql="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 s3_region="",
                 s3_jsondetails="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_creds = aws_creds
        self.creastesql = createsql
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.s3_jsondetails = s3_jsondetails

    def execute(self, context):
        aws_hook = AwsHook(self.aws_creds)
        creds = aws_hook.getcredentials()
        rs_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Dropping old stagings tables!")
        dropst = dropsql(self.table)
        rs_hook.run(dropst)
        
        self.log.info("Creating staging table")
        rs_hook.run(self.createsql)
        
        self.log.info("Copying data from S3 to Redshift for " + self.table)
        s3_path = "s3://" + self.bucket + "/" + self.s3_key
        formated_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                creds.access_key,
                creds.secret_key,
                self.s3_region)
        rs_hook.run(formated_sql)
        
        self.log.info('StageToRedshiftOperator finished!')





