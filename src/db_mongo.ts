import * as dotenv from 'dotenv';
dotenv.config();

import * as mongodb from 'mongodb';

const mongodb_credential = process.env.AOZORA_MONGODB_CREDENTIAL || '';
const mongodb_host = process.env.AOZORA_MONGODB_HOST || 'localhost';
const mongodb_port = process.env.AOZORA_MONGODB_PORT || '27017';
const mongodb_replica_set = process.env.AOZORA_MONGODB_REPLICA_SET;
const mongo_url = `mongodb://${mongodb_credential}${mongodb_host}:${mongodb_port}/aozora`;

import type { Book as BookBase, Person as PersonBase, StringMap } from './models';
import { separate_obj } from './models';

//
// utilities
//
type Book = BookBase & {
    [index: string]: string | StringMap[];
};

type Person = PersonBase & {
    [index: string]: string | StringMap[];
};

type BookList = { [key: string]: Book };
type PersonList = { [key: string]: Person };

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

    public async updated(data: string[], refresh: boolean): Promise<string[]> {
        if (refresh) {
            return data;
        }

        const books = this._collection('books');
        const the_latest_item = await books.findOne(
            {},
            { projection: { release_date: 1 }, sort: { release_date: -1 } }
        );
        const last_release_date = the_latest_item
            ? the_latest_item.release_date
            : new Date('1970-01-01');

        return data.slice(1).filter(entry => {
            return last_release_date < new Date(entry[11]);
        });
    }

    public async import_books_persons(updated: any): Promise<number> {
        const books_batch_list: BookList = {};
        const persons_batch_list: PersonList = {};
        await Promise.all(
            updated.map((entry: StringMap) => {
                const { book, role, person } = separate_obj(entry);
                if (!books_batch_list[book.book_id]) {
                    // book.persons = [];
                    books_batch_list[book.book_id] = book;
                }
                if (!books_batch_list[book.book_id][role]) {
                    books_batch_list[book.book_id][role] = [];
                }
                (books_batch_list[book.book_id][role] as StringMap[]).push({
                    first_name: person.first_name,
                    last_name: person.last_name,
                    full_name: person.last_name + person.first_name,
                    person_id: person.person_id
                });
                if (!persons_batch_list[person.person_id]) {
                    person.full_name = person.last_name + person.first_name;
                    persons_batch_list[person.person_id] = person;
                }
            })
        );

        return Promise.all([
            this._store_books(books_batch_list),
            this._store_persons(persons_batch_list)
        ]).then(res => {
            return res[0].upsertedCount + res[0].modifiedCount;
        });
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

    private async _store_books(
        books_batch_list: BookList
    ): Promise<mongodb.BulkWriteOpResultObject> {
        const books = this._collection('books');
        const operations = Object.keys(books_batch_list).map(book_id => {
            const book = books_batch_list[book_id];
            return { updateOne: { filter: { book_id: book.book_id }, update: {$set: book}, upsert: true } };
        });
        const options: mongodb.CollectionBulkWriteOptions = { ordered: false };
        return books.bulkWrite(operations, options);
    }

    private async _store_persons(
        persons_batch_list: PersonList
    ): Promise<mongodb.BulkWriteOpResultObject> {
        const persons = this._collection('persons');
        const operations = Object.keys(persons_batch_list).map(person_id => {
            const person = persons_batch_list[person_id];
            return {
                updateOne: { filter: { person_id: person.person_id }, update: {$set: person}, upsert: true }
            };
        });
        const options: mongodb.CollectionBulkWriteOptions = { ordered: false };
        return persons.bulkWrite(operations, options);
    }
}

export function make_db(): IDB {
    return new DB();
}
