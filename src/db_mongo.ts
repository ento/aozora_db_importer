import * as dotenv from 'dotenv';
dotenv.config();

import * as mongodb from 'mongodb';

const mongodb_credential = process.env.AOZORA_MONGODB_CREDENTIAL || '';
const mongodb_host = process.env.AOZORA_MONGODB_HOST || 'localhost';
const mongodb_port = process.env.AOZORA_MONGODB_PORT || '27017';
const mongodb_replica_set = process.env.AOZORA_MONGODB_REPLICA_SET;
const mongo_url = `mongodb://${mongodb_credential}${mongodb_host}:${mongodb_port}/aozora`;

import type { Book as BookBase, Person as PersonBase, StringMap } from './models';

//
// utilities
//
type Book = BookBase & {
    [index: string]: string | StringMap[];
};

type Person = PersonBase & {
    [index: string]: string | StringMap[];
};

import { IDB } from './i_db';

class DB implements IDB {
    private client: mongodb.MongoClient;

    public async connect(): Promise<void> {
        const options: mongodb.MongoClientOptions = {
            useNewUrlParser: true,
            useUnifiedTopology: true
        };
        if (mongodb_replica_set) {
            options.ssl = true;
            options.replicaSet = mongodb_replica_set;
            options.authMechanism = 'SCRAM-SHA-1';
            options.authSource = 'admin';
        }
        // console.log(mongo_url, options);
        this.client = await mongodb.MongoClient.connect(mongo_url, options);
    }

    public async get_last_release_date(): Promise<Date> {
        const books = this._collection('books');
        const the_latest_item = await books.findOne(
            {},
            { projection: { release_date: 1 }, sort: { release_date: -1 } }
        );
        return the_latest_item
            ? the_latest_item.release_date
            : new Date('1970-01-01');
    }

    public async import_byname(name: string, bulk_ops: any): Promise<number> {
        const result = await this._collection(name).bulkWrite(bulk_ops);
        return result.upsertedCount;
    }

    public async find_bookids(collection: string, query: object): Promise<number[]> {
        return this._collection(collection)
            .find(query)
            .map((e: { book_id: number }) => e.book_id)
            .toArray();
    }

    public find_one<T>(collection: string, query: object): Promise<T> {
        return this._collection(collection).findOne<T>(query);
    }

    public create_index(collection: string, spec: object, options: object): Promise<string> {
        return this._collection(collection).createIndex(spec, options);
    }

    public replace_one(
        collection: string,
        filter: object,
        doc: object,
        options: object
    ): Promise<object> {
        return this._collection(collection).replaceOne(filter, doc, options);
    }

    public async close(): Promise<void> {
        this.client.close();
    }

    private _collection(name: string): mongodb.Collection<any> {
        return this.client.db().collection(name);
    }

    public async store_books(
        books_batch_list: Record<string, Book>
    ): Promise<number> {
        const books = this._collection('books');
        const operations = Object.keys(books_batch_list).map(book_id => {
            const book = books_batch_list[book_id];
            return { updateOne: { filter: { book_id: book.book_id }, update: {$set: book}, upsert: true } };
        });
        const options: mongodb.CollectionBulkWriteOptions = { ordered: false };
        const result = await books.bulkWrite(operations, options);
        return result.upsertedCount + result.modifiedCount;
    }

    public async store_persons(
        persons_batch_list: Record<string, Person>
    ): Promise<number> {
        const persons = this._collection('persons');
        const operations = Object.keys(persons_batch_list).map(person_id => {
            const person = persons_batch_list[person_id];
            return {
                updateOne: { filter: { person_id: person.person_id }, update: {$set: person}, upsert: true }
            };
        });
        const options: mongodb.CollectionBulkWriteOptions = { ordered: false };
        const result = persons.bulkWrite(operations, options);
        return result.upsertedCount + result.modifiedCount;
    }
}

export function make_db(): IDB {
    return new DB();
}
