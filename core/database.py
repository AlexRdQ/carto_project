import psycopg2

class Database:
    def __init__(self):
        self.conn = psycopg2.connect(user="client", password="password", 
                                    host="localhost", port="5432", database="carto_project")
        self.cur = self.conn.cursor()

    def execute(self, query, params=None):
        if params is None:
            self.cur.execute(query)
        else:
            self.cur.execute(query, params)
        self.conn.commit()

    def fetchall(self):
        return self.cur.fetchall()

    def fetchone(self):
        return self.cur.fetchone()

    def close(self):
        self.cur.close()
        self.conn.close()
