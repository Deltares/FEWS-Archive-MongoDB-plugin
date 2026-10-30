// In-memory stand-in for the Spring GraphQL backend, used only by
// vite.mock.config.js via "npm run dev:mock", so the UI can be developed offline.
// Never imported by vite.config.js and never reachable from a build.
//
// Records live in memory for the life of the dev server; editing this file
// restarts Vite and resets them to the seed data below.

const settings = {
  toEmailAddresses: 'ops@tva.gov',
  fromEmailAddress: 'noreply@tva.gov',
  drdlYamlPath: 'D:/drdl',
  fewsRestApiUri: 'https://fews.example/rest',
  environment: 'PRODUCTION',
  cubeAdmins: 'TVA\\admins',
  cubeUsers: 'TVA\\users',
  bimPath: 'D:/bim',
  databaseConnectionString: 'mongodb://localhost:27017',
  smtpServer: 'smtp.tva.gov',
  smtpPort: 25,
  smtpUser: 'svc',
  smtpPass: '***',
  tabularConnectionString: 'Provider=MSOLAP;',
  execute: true,
  reprocessCubes: 'none',
  taskInterval: 'PT1H',
  fewsArchiveDbAesPassword: '***',
  fewsArchiveDbUsername: 'archive',
  fewsArchiveDbConnection: 'jdbc:postgresql://localhost/archive',
  dataStaleAfterSeconds: 90,
  cubeParallelPartitions: 4,
  cubeThreads: 8,
  dataParallelPartitions: 4,
  dataThreads: 8,
  fewsVerificationDbAesPassword: '***',
  fewsVerificationDbConnection: 'jdbc:postgresql://localhost/verify',
  fewsVerificationDbUsername: 'verify',
  parallel: true,
  processData: true,
}

const store = {
  classN: [
    {_id: '1', Name: 'Tributaries', Locations: ['CHAT', 'NICK', 'OCOE']},
    {_id: '2', Name: 'Mainstem', Locations: ['KENT', 'PICK', 'WATT']},
  ],
  seasonalityN: [
    {
      _id: '1',
      Name: 'Quarterly',
      Breakpoint: [
        {Name: 'Q1', Months: [1, 2, 3]},
        {Name: 'Q2', Months: [4, 5, 6]},
      ],
    },
  ],
  locationAttributesN: [{_id: '1', Name: 'Standard', Attributes: {Basin: 'string', DrainageArea: 'number'}}],
  templateCubeN: [{_id: '1', Name: 'DefaultCube', Template: {model: {tables: []}}}],
  templateDrdlYamlN: [{_id: '1', Type: 'fact', Name: 'ForecastObserved', Template: 'table: fact\ncolumns:\n  - name: _id'}],
  templatePowerQueryN: [{_id: '1', Name: 'DefaultQuery', Template: 'let Source = Sql.Database() in Source'}],
  outputPowerQueryN: [{_id: '1', Study: 'DemoStudy', Name: 'Summary', Month: '2026-01', Expression: 'let Source = ... in Source'}],
  dimensionIsOriginalForecastN: [
    {_id: '1', isOriginalForecast: true},
    {_id: '2', isOriginalForecast: false},
  ],
  dimensionIsOriginalObservedN: [
    {_id: '1', isOriginalObserved: true},
    {_id: '2', isOriginalObserved: false},
  ],
  dimensionMeasureN: [
    {_id: '1', measureId: 'MAE', measure: 'Mean Absolute Error', perfectScore: 0},
    {_id: '2', measureId: 'NSE', measure: 'Nash-Sutcliffe Efficiency', perfectScore: 1},
  ],
  fewsLocationsN: [
    {
      _id: '1',
      lastUpdated: '2026-08-27T12:00:00Z',
      locations: [
        {id: 'CHAT', name: 'Chatuge'},
        {id: 'NICK', name: 'Nickajack'},
      ],
    },
  ],
  fewsParametersN: [
    {
      _id: '1',
      lastUpdated: '2026-08-27T12:00:00Z',
      parameters: [
        {id: 'Q', name: 'Discharge'},
        {id: 'H', name: 'Stage'},
      ],
    },
  ],
  fewsQualifiersN: [{_id: '1', lastUpdated: '2026-08-27T12:00:00Z', qualifiers: [{id: 'OBS', name: 'Observed'}]}],
  normalN: [{_id: '1', Name: 'DailyNormal', Collection: 'normals', Filters: [{FilterName: 'basin', Value: 'TN'}]}],
  observedN: [{_id: '1', Name: 'HourlyObserved', Collection: 'observed', Filters: [{FilterName: 'basin', Value: 'TN'}]}],
  forecastN: [
    {
      _id: '1',
      Name: 'DayAhead',
      ForecastName: 'DA',
      Collection: 'forecasts',
      Filters: [{FilterName: 'basin', Value: 'TN'}],
    },
  ],
  studyN: [
    {
      _id: '1',
      Name: 'DemoStudy',
      Observed: 'HourlyObserved',
      Forecasts: ['DayAhead'],
      Seasonalities: ['Quarterly'],
      Class: 'Tributaries',
      LocationAttributes: 'Standard',
      ForecastStartMonth: '2026-01',
      ForecastEndMonth: '2026-12',
      Time: 'UTC',
      Value: 'native',
      Normal: 'DailyNormal',
      Cube: 'DefaultCube',
      Active: true,
      ReprocessDays: 7,
      MaxLeadTimeMinutes: 10080,
    },
  ],
  configurationSettingsN: [{_id: '1', ...settings}],
  configurationDescriptionN: [{_id: '1', Name: 'Defaults', ...Object.fromEntries(Object.keys(settings).map((k) => [k, `Description of ${k}`]))}],
}

let nextId = 100

const collectionFor = (field) => {
  const entity = field.replace(/^(create|update|delete)/, '')
  return `${entity[0].toLowerCase()}${entity.slice(1)}N`
}

// Mutation arguments are camelCase (`name`, `_class`) while stored records use
// each collection's own casing (`Name`, `Class`). Rather than guess, match each
// argument case-insensitively against the field names an existing record already
// has, so a write reuses the real key instead of inventing a second one.
const toRecord = (_id, vars, template = {}) => {
  const fields = Object.keys(template)
  const record = {_id}
  for (const [key, value] of Object.entries(vars)) {
    if (key === '_id') continue
    const bare = key.replace(/^_/, '')
    record[fields.find((f) => f.toLowerCase() === bare.toLowerCase()) ?? bare] = value
  }
  return record
}

function resolve(field, vars) {
  if (field === 'user') return {user: {Name: 'Test User', Email: 'test.user@tva.gov'}}
  if (field === 'version') return {version: {Version: '1.0-mock'}}
  if (field.endsWith('Test')) {
    return {
      [field]: [
        {FilterName: 'basin', Success: 'true'},
        {FilterName: 'parameter', Success: 'true'},
      ],
    }
  }
  if (field in store) return {[field]: store[field]}

  const collection = collectionFor(field)
  if (!(collection in store)) throw new Error(`No mock handler for "${field}"`)

  // Any existing row shows what this collection's field names actually look like.
  const template = store[collection][0] ?? {}

  if (field.startsWith('create')) {
    const _id = String(nextId++)
    store[collection].push(toRecord(_id, vars, template))
    return {[field]: _id}
  }
  if (field.startsWith('update')) {
    const index = store[collection].findIndex((r) => r._id === vars._id)
    if (index < 0) throw new Error(`No ${collection} record with _id ${vars._id}`)
    store[collection][index] = toRecord(vars._id, vars, template)
    return {[field]: vars._id}
  }
  store[collection] = store[collection].filter((r) => r._id !== vars._id)
  return {[field]: vars._id}
}

export function mockGraphql() {
  return {
    name: 'mock-graphql',
    apply: 'serve',
    configureServer(server) {
      server.middlewares.use('/graphql', (req, res) => {
        let body = ''
        req.on('data', (chunk) => (body += chunk))
        req.on('end', () => {
          const {query, variables} = JSON.parse(body)
          res.setHeader('content-type', 'application/json')
          try {
            // A document may select several root fields (StudyPage does); merge them.
            const fields = [...query.matchAll(/[{\n]\s*(\w+)\s*[({]/g)].map((m) => m[1])
            const data = Object.assign({}, ...fields.map((f) => resolve(f, variables ?? {})))
            res.end(JSON.stringify({data}))
          } catch (e) {
            res.end(JSON.stringify({errors: [{message: e.message}]}))
          }
        })
      })
    },
  }
}
