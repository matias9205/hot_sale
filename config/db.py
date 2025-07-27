import pyodbc

class DbConnection:
    def __init__(self, db_url_: str):
        self.db_url = db_url_
        self.conn = None
        self.cursor = None
        self.get_connection()

    def get_connection(self):
        try:
            self.conn = pyodbc.connect(self.db_url)
            self.cursor = self.conn.cursor()
            print("✅ Connection and cursor established.")
        except pyodbc.Error as ex:
            print(f"❌ Connection failed: {self.db_url}")
            print(f"Error: {ex}")
            self.conn = None
            self.cursor = None

    def test_connection(self):
        if self.conn:
            try:
                self.cursor = self.conn.cursor()
                self.cursor.execute("SELECT 1")
                print("✅ Test query executed successfully.")
            except pyodbc.Error as ex:
                print(f"❌ Test query failed: {ex}")


    def get_cursor(self):
        if self.conn is None:
            print("⚠️ No active connection.")
            return None
        if self.cursor is None:
            try:
                self.cursor = self.conn.cursor()
                print("✅ Cursor initialized.")
            except pyodbc.Error as ex:
                print(f"❌ Failed to create cursor: {ex}")
                return None
        return self.cursor
    
    def close(self):
        if self.cursor:
            try:
                self.cursor.close()
                print("✅ Cursor closed.")
            except pyodbc.Error as ex:
                print(f"❌ Error closing cursor: {ex}")
        if self.conn:
            try:
                self.conn.close()
                print("✅ Connection closed.")
            except pyodbc.Error as ex:
                print(f"❌ Error closing connection: {ex}")