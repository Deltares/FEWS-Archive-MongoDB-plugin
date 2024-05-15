//use FEWS_ARCHIVE
function getType(v){
    let type = typeof(v);
    if (!v || type === 'string')
        type = 'String';
    else if (type === 'boolean')
        type = 'Boolean';
    else if (type === 'object')
        type = v instanceof ObjectId ? 'ID' : Array.isArray(v) ? `[${v.length ? getType(v[0]) : 'Json'}]` : 'Json';
    else if (type === 'number')
        type = v % 1 === 0 ? 'Int' : 'Float';
    return type;
}

function getPascalCase(s){
    return `${s[0].toUpperCase()}${s.slice(1)}`;
}

function getCamelCase(s){
    return `${s[0].toLowerCase()}${s.slice(1)}`;
}

function getQueryName(c, k, u){
    c = c.replace('.', '');
    return `${getCamelCase(c)}${u ? '' : 'N'}${k.length ? 'By' : ''}${k.map(n => {let s = n.replace('_', ''); return getPascalCase(s);}).join('')}`;
}

function getQueryArray(c, i){
    c = getQueryType(c);
    return i.unique || i.name === '_id_' ? c : `[${c}]`;
}

function getQueryType(c){
    return getPascalCase(c.replace('.', ''));
}

db.getCollectionNames().forEach(c => {
    
    let typeLookup = Object.entries(db.getCollection(c).find().limit(500).toArray().reduce((a, v) => Object.assign(a, v), {})).reduce((a, [k, v]) => {a[k] = getType(v); return a;}, {});
    
    let s = Object.entries(typeLookup).map(([k, v]) => `\n\t${k}: ${v}!`).join('');
    print(`\n\ntype ${getQueryType(c)} {${s}\n}`);
    
    let q = db.getCollection(c).getIndexes().map(i => `\n\t${getQueryName(c, Object.keys(i.key), i.unique || i.name === '_id_')}(${Object.keys(i.key).map(l => `${getCamelCase(l)}: ${typeLookup[l]}!`).join(',')}): ${getQueryArray(c, i)}`).join('');
    q = `${q}\n\t${getQueryName(c,[],false)}: ${getQueryArray(c, {})}`;
    print(`type Query {${q}\n}`);
    
    let m = `\n\tcreate${getQueryType(c)}(document: Json!): ID\n\tupdate${getQueryType(c)}(document: Json!): Int\n\tdelete${getQueryType(c)}(_id: ID!): Int`;
    print(`type Mutation {${m}\n}`);
});
