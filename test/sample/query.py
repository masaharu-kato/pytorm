from schema import SampleDB as db
from connector import sample_connector

def main():
    db.connect(sample_connector)

    records = db.select([
        db.Item.id,
        db.Item >> db.Group >> db.Category.name,
        db.Item >> db.Group.name,
        db.Item.name,
    ])

    for record in records:
        print('\t'.join(record))


if __name__ == "__main__":
    main()

