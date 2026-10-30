// Offline development. Wraps the real vite.config.js and adds the in-memory
// /graphql mock from mock/graphql.js, so the UI can be run and clicked through
// with no backend and no network - on a plane, or off the corporate network.
//
//   npm run dev:mock
//
// This file is the only thing that references the mock. vite.config.js does not
// import it, so `npm run dev`, `npm run build` and the Maven build behave exactly
// as if it were not here. The mock plugin is also apply:'serve', so it could not
// reach a production bundle even if something did import it.
import {mergeConfig} from 'vite'
import baseConfig from './vite.config.js'
import {mockGraphql} from './mock/graphql.js'

export default (env) => mergeConfig(baseConfig(env), {plugins: [mockGraphql()]})
