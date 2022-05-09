import boto3
from botocore.exceptions import ClientError
import ast


class NoblrSecrets:

    def __init__(self, secret_name, secret_region):
        self.secret_name = secret_name
        self.secret_region = secret_region
        session = boto3.session.Session()
        self.client = session.client(
            service_name='secretsmanager',
            region_name=self.secret_region
        )

    def get_secret(self):
        try:
            get_secret_value_response = self.client.get_secret_value(
                SecretId=self.secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print("The requested secret " + secret_name + " was not found")
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                print("The request was invalid due to:", e)
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                print("The request had invalid params:", e)
            elif e.response['Error']['Code'] == 'DecryptionFailure':
                print("The requested secret can't be decrypted using the provided KMS key:", e)
            elif e.response['Error']['Code'] == 'InternalServiceError':
                print("An error occurred on service side:", e)
        else:
            # Secrets Manager decrypts the secret value using the associated KMS CMK
            # Depending on whether the secret was a string or binary, only one of these fields will be populated
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                return ast.literal_eval(secret)

