from subprocess import PIPE,Popen

# creating .dmp file for specified table in specified database
def dump_table(host_name,port,database_name,user_name,database_password,schema_name, table_name, dump_file_loc):

    command = f'pg_dump -d postgres://{user_name}:{database_password}@{host_name}:{port}/{database_name} -t {schema_name}.{table_name} -Fc -f {dump_file_loc}'

    p = Popen(command,shell=True,stdin=PIPE,stdout=PIPE,stderr=PIPE, encoding='utf8')

    return p.communicate()

# use .dmp file created by dump_table() to re-create table in another database
def restore_table(host_name,port,database_name,user_name,database_password,dump_file_loc):

    command = f'pg_restore -d postgres://{user_name}:{database_password}@{host_name}:{port}/{database_name} {dump_file_loc}'

    p = Popen(command,shell=True,stdin=PIPE,encoding='utf8')

    return p.communicate()