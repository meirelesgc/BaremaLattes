import psycopg
import psycopg.rows


class Connection:
    def __init__(self, conninfo):
        self.conninfo = conninfo

    def exec(self, query, params=None):
        conn = None
        cur = None
        try:
            conn = psycopg.connect(self.conninfo)
            cur = conn.cursor(row_factory=psycopg.rows.dict_row)
            cur.execute(query, params)
            conn.commit()
            return cur.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"\n[ERROR] Executando query: {query}")
            print(f"Parâmetros: {params}")
            print(f"Erro: {e}\n")
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    def execmany(self, query, params=None):
        conn = None
        cur = None
        try:
            conn = psycopg.connect(self.conninfo)
            cur = conn.cursor(row_factory=psycopg.rows.dict_row)
            cur.executemany(query, params)
            conn.commit()
            return cur.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"\n[ERROR] Executando query: {query}")
            print(f"Parâmetros: {params}")
            print(f"Erro: {e}\n")
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    def select(self, query, params=None, one=False):
        conn = None
        cur = None
        try:
            conn = psycopg.connect(self.conninfo)
            cur = conn.cursor(row_factory=psycopg.rows.dict_row)
            cur.execute(query, params)
            result = cur.fetchone() if one else cur.fetchall()
            return result
        except Exception as e:
            print(f"\n[ERROR] Executando query: {query}")
            print(f"Parâmetros: {params}")
            print(f"Erro: {e}\n")
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    def close(self):
        pass
