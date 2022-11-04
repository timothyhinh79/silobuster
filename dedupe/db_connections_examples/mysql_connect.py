from pymysql import connect


def get_session():
    connection = connect(
                            host='ms-mysql',
                            user='root',
                            password='example',
                        )
    return connection

def check_connection():
    with get_session().cursor() as cursor:
        data = cursor.execute("SELECT CONNECTION_ID")
        return data[0][0]
