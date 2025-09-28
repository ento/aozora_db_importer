import http from 'http';
import https from 'https';

import * as dotenv from 'dotenv';
dotenv.config();

import axios, { AxiosInstance } from 'axios';

import { IDB } from './i_db';
import type { Book, Person } from './models';

const booksUrl = 'https://ento-aozora-api.builtwithdark.com/books';
const personsUrl = 'https://ento-aozora-api.builtwithdark.com/persons';
const lastModifiedUrl = 'https://ento-aozora-api.builtwithdark.com/books/last-modified';

class DB implements IDB {
    private client: AxiosInstance;

    public async connect(): Promise<void> {
        this.client = axios.create({
            httpAgent: new http.Agent({
                keepAlive: true,
                maxSockets: 10,
                maxFreeSockets: 10,
                timeout: 60000, // active socket
            }),
            httpsAgent: new https.Agent({
                keepAlive: true,
                maxSockets: 10,
                maxFreeSockets: 10,
                timeout: 60000, // active socket
            }),
        });
    }

    public async get_last_release_date(): Promise<Date> {
        return new Date(0);
        //const res = await this.client.get(lastModifiedUrl);
        //return new Date(res.data);
    }

    public async close(): Promise<void> {
    }

    public async store_books(books: Record<string, Book>): Promise<number> {
        const bookList = Object.values(books);
        const requests = bookList.map((book) => this.client.put(booksUrl, book).then(() => console.log('stored book', book.book_id)));
        return Promise.allSettled(requests).then((results) => {
            results.forEach((result, i) => {
                if (result.status === 'rejected') {
                    console.error('failed to store book', bookList[i].book_id, result.reason.code, result.reason.cause);
                }
            })
            return results.filter((result) => result.status === 'fulfilled').length;
        });
    }

    public async store_persons(persons: Record<string, Person>): Promise<number> {
        return 0;
        const personList = Object.values(persons);
        const requests = personList.map((person) => this.client.put(personsUrl, person).then(() => console.log('stored person', person.person_id)));
        return Promise.allSettled(requests).then((results) => {
            results.forEach((result, i) => {
                if (result.status === 'rejected') {
                    console.error('failed to store person', personList[i].person_id, result.reason.code, result.reason.cause);
                }
            })
            return results.filter((result) => result.status === 'fulfilled').length;
        });
    }
}

export function make_db(): IDB {
    return new DB();
}
