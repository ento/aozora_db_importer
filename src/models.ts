import { ATTRS } from './attrs';

export type StringMap = Record<string, string>;

export type Book = {
    book_id?: string;
    title?: string;
    title_yomi?: string;
    title_sort?: string;
    subtitle?: string;
    subtitle_yomi?: string;
    original_title?: string;
    first_appearance?: string;
    ndc_code?: string;
    font_kana_type?: string;
    copyright?: string;
    release_date?: string;
    last_modified?: string;
    card_url?: string;
    revisers?: StringMap[];
    editors?: StringMap[];
    translators?: StringMap[];
    authors?: StringMap[];
    base_book_1?: string;
    base_book_1_publisher?: string;
    base_book_1_1st_edition?: string;
    base_book_1_edition_input?: string;
    base_book_1_edition_proofing?: string;
    base_book_1_parent?: string;
    base_book_1_parent_publisher?: string;
    base_book_1_parent_1st_edition?: string;
    base_book_2?: string;
    base_book_2_publisher?: string;
    base_book_2_1st_edition?: string;
    base_book_2_edition_input?: string;
    base_book_2_edition_proofing?: string;
    base_book_2_parent?: string;
    base_book_2_parent_publisher?: string;
    base_book_2_parent_1st_edition?: string;
    input?: string;
    proofing?: string;
    text_url?: string;
    text_last_modified?: string;
    text_encoding?: string;
    text_charset?: string;
    text_updated?: string;
    html_url?: string;
    html_last_modified?: string;
    html_encoding?: string;
    html_charset?: string;
    html_updated?: string;
};

export type Person = {
    person_id?: string;
    first_name?: string;
    last_name?: string;
    last_name_yomi?: string;
    first_name_yomi?: string;
    last_name_sort?: string;
    first_name_sort?: string;
    last_name_roman?: string;
    first_name_roman?: string;
    date_of_birth?: string;
    date_of_death?: string;
    author_copyright?: string;
};

type BookRolePerson = {
    book: Book;
    role: string;
    person: Person;
};

export function separate_obj(entry: StringMap): BookRolePerson {
    const book: Book = {};
    const person: Person = {};
    let role = null;

    ATTRS.forEach((e, i) => {
        const value = entry[i];

        if (
            [
                'person_id',
                'first_name',
                'last_name',
                'last_name_yomi',
                'first_name_yomi',
                'last_name_sort',
                'first_name_sort',
                'last_name_roman',
                'first_name_roman',
                'date_of_birth',
                'date_of_death',
                'author_copyright'
            ].includes(e)
        ) {
            person[e] = value;
        } else if (e === 'role') {
            role = value;
        } else {
            book[e] = value;
        }
    });
    return { book, role, person };
}
