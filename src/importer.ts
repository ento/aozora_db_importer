import { IDB } from './i_db';
import type { Book, Person } from './models';
import { ROLE_KEYS, separate_obj } from './models';

export class Importer {
    private backend: IDB;

    constructor(backend: IDB) {
        this.backend = backend;
    }

    public async connect(): Promise<void> {
        return this.backend.connect();
    }

    public async close(): Promise<void> {
        this.backend.close();
    }

    public async updated(data: Record<string, string>[], refresh: boolean): Promise<Record<string, string>[]> {
        if (refresh) {
            return data;
        }

        const last_release_date = await this.backend.get_last_release_date();

        return data.slice(1).filter(entry => {
            const released = new Date(entry[11]);
            const modified = new Date(entry[12]);
            const date_to_compare = released < modified ? modified : released;
            return last_release_date < date_to_compare;
        });
    }

    public async import_books_persons(updated: Record<string, string>[]): Promise<number> {
        const books_batch_list: Record<string, Book> = {};
        const persons_batch_list: Record<string, Person> = {};
        updated.forEach((entry: Record<string, string>) => {
            const { book, role, person } = separate_obj(entry);
            if (!books_batch_list[book.book_id]) {
                ROLE_KEYS.forEach(role_key => {
                    book[role_key] = [];
                });
                books_batch_list[book.book_id] = book;
            }
            if (!books_batch_list[book.book_id][role]) {
                console.log(`Role ${role} not found for book ${book.book_id}`, entry);
            }
            (books_batch_list[book.book_id][role]).push({
                first_name: person.first_name,
                last_name: person.last_name,
                full_name: person.last_name + person.first_name,
                person_id: person.person_id
            });
            if (!persons_batch_list[person.person_id]) {
                person.full_name = person.last_name + person.first_name;
                persons_batch_list[person.person_id] = person;
            }
        });

        return Promise.all([
            this.backend.store_books(books_batch_list),
            this.backend.store_persons(persons_batch_list)
        ]).then(res => {
            return res.reduce((a, b) => a + b, 0);
        });
    }
}
