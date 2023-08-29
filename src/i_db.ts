import type { Book, Person } from './models';

export interface IDB {
    connect(): Promise<void>;
    close(): Promise<void>;

    get_last_release_date(): Promise<Date>;
    store_books(books: Record<string, Book>): Promise<number>;
    store_persons(persons: Record<string, Person>): Promise<number>;

    import_byname?(name: string, bulk_ops: any): Promise<number>;

    find_bookids?(collection: string, query: object): Promise<number[]>;
    find_one?<T>(collection: string, query: object): Promise<T>;

    replace_one?(collection: string, filter: object, doc: object, options: object): Promise<object>;

    create_index?(collection: string, spec: object, options: object): Promise<string>;
}
