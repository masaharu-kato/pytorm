import schema as sc
from connector import connector

def main():
    with connector as connection:
        with connection.operate(sc.db) as operation:
            operation.execute(sc.Item.select())



if __name__ == "__main__":
    main()

