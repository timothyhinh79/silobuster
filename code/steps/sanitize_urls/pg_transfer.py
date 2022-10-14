from subprocess import PIPE,Popen

# consider passing PGPASSWORD in as environment variable, along with default env from os.environ so that it doesn't error out?

def dump_table(host_name,port,database_name,user_name,database_password,schema_name, table_name, dump_file_loc):

    command = f'pg_dump -d postgres://{user_name}:{database_password}@{host_name}:{port}/{database_name} -t {schema_name}.{table_name} -Fc -f {dump_file_loc}'

    p = Popen(command,shell=True,stdin=PIPE,stdout=PIPE,stderr=PIPE, encoding='utf8')

    return p.communicate()

def restore_table(host_name,port,database_name,user_name,database_password,dump_file_loc):

    command = f'pg_restore -d postgres://{user_name}:{database_password}@{host_name}:{port}/{database_name} {dump_file_loc}'

    p = Popen(command,shell=True,stdin=PIPE,encoding='utf8')

    return p.communicate()