const rp = require('request-promise');
const cheerio = require('cheerio');

require('dotenv').config();

const mongodb = require('mongodb');
const mongodb_credential = process.env.AOZORA_MONGODB_CREDENTIAL || '';
const mongodb_host = process.env.AOZORA_MONGODB_HOST || 'localhost';
const mongodb_port = process.env.AOZORA_MONGODB_PORT || '27017';
const mongo_url = `mongodb://${mongodb_credential}${mongodb_host}:${mongodb_port}/aozora`;

const transform = (body) => {
  const $ = cheerio.load(body);
  const items = $('tr[valign]').map(function() {
    return {id: parseInt($(this).find(':nth-child(1)').text().trim()),
            name: $(this).find(':nth-child(2)').text().trim().replace('　',' ')};
  }).get().slice(1);

  return items.map((item)=> {
    return {updateOne: {filter: {id: item.id},
                        update: item,
                        upsert: true}};
  });
};

const idurls = {
  // 'persons': 'http://reception.aozora.gr.jp/pidlist.php?page=1&pagerow=-1',
  'workers': 'http://reception.aozora.gr.jp/widlist.php?page=1&pagerow=-1'
};

const run = async () => {
  for(let idname in idurls) {
    const idurl = idurls[idname];
    const options = {
      url: idurl,
      transform: transform
    };

    const bulk_ops = await rp.get(options);

    const client = await mongodb.MongoClient.connect(mongo_url, {useNewUrlParser: true});
    const result = await client.db().collection(idname).bulkWrite(bulk_ops);
    console.log(`updated ${result.upsertedCount} entries`); // eslint-disable-line no-console
    client.close();
  }
};

run();
