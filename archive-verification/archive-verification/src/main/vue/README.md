# Verification UI

The browser front end for the FEWS verification service. It is a Vue 3 single-page
app that is **built into the Spring Boot WAR** rather than deployed separately.

## How it fits the Maven build

`pom.xml` drives this project through `exec-maven-plugin`:

| Phase                | Runs                                    | If it fails                                |
| -------------------- | --------------------------------------- | ------------------------------------------ |
| `generate-resources` | `npm install`                           | build fails                                |
| `generate-resources` | `npm run lint`                          | **build fails**                            |
| `generate-resources` | `npm run build` → `../resources/static` | build fails                                |
| `test`               | `npm test`                              | **build fails** (skipped by `-DskipTests`) |

So a lint error or a failing front-end test fails `mvn package`, the same as a Java
compile error would. Run `npm run verify` before committing to catch both locally.

`mvn package` produces one WAR containing the API and this UI. There is no separate
front-end deployment and no API base URL to configure: the app is served from the
same origin as the API, so `fetch('./graphql')` resolves wherever the WAR is mounted.
That is what `base: "./"` in `vite.config.js` protects — do not make it absolute.

The build output goes to `../resources/static`, which is generated, gitignored, and
should never be edited by hand.

## Running it

| Command                         | Does                                                         |
| ------------------------------- | ------------------------------------------------------------ |
| `npm run dev`                   | Dev server with hot reload. **No backend** — see below       |
| `npm run dev:mock`              | Dev server plus an in-memory `/graphql`, for working offline |
| `npm run build`                 | Production build into `../resources/static`                  |
| `npm run build:debug`           | Same, unminified and with sourcemaps                         |
| `npm run preview`               | Serve the built output locally                               |
| `npm test`                      | Vitest suite                                                 |
| `npm run lint` / `lint:fix`     | ESLint, check-only / autofix                                 |
| `npm run format` / `format:fix` | Prettier, check-only / rewrite                               |
| `npm run verify`                | lint + format + test, as the build runs them                 |

`npm run dev` has nothing answering `/graphql`, so pages show a connection error.
Two ways to get data:

- **`npm run dev:mock`** — serves seeded records from `mock/graphql.js`. Everything
  is in memory and resets when the server restarts. Good for UI work with no
  network. It writes loosely, so verify anything schema-sensitive against the real
  API before trusting it.
- **The real thing** — build the WAR and run Spring Boot, which serves both halves
  together. This is the only way to exercise mutations against the real schema.

The mock is reachable only through `vite.mock.config.js`. `vite.config.js` never
imports it, and the plugin is `apply: 'serve'`, so it cannot reach a build.

## Layout

```
src/
  main.js               app entry: router, Vuetify, global CSS
  App.vue               shell - app bar, nav drawer, clock
  graphql.js            the entire data layer (one fetch call)
  navigation.js         the nav tree, shared by App.vue and HomePage.vue
  vuetify.js            Vuetify setup - components are auto-imported, see below
  router/index.js       routes, each a lazy import
  components/           shared UI, with __tests__ alongside
  composables/          shared logic, with __tests__ alongside
  views/                one component per route
  assets/main.css       app-wide styles
mock/graphql.js         offline fixture, dev only
```

Unit tests live in `__tests__/` folders beside what they test, which is the Vue
convention. Note that this differs from the Java half of the repo, where Maven
mirrors `src/main/java` into `src/test/java` — each language follows its own norm.

## Conventions

**Data access.** Every request goes through `graphql(query, variables)` in
`graphql.js`. It POSTs to `./graphql`, throws on a non-2xx response or a GraphQL
`errors[]`, and returns the `data` payload. There is no client cache: pages refetch
after a mutation. Queries are plain template strings in `const`s at the top of each
view — keep them there rather than inline, or Prettier reformats them into
multi-line blocks.

**Page shape.** Every editor view calls `useEditor(LIST, collection)`, which owns
`loading` / `error` / `success` / `warning` and the selected row. Its `run()` takes
no argument to reload, or a function to mutate-then-reload. Copy an existing view
rather than inventing a new shape.

**Components.**

|                            |                                                                                                           |
| -------------------------- | --------------------------------------------------------------------------------------------------------- |
| `StatusBar`                | loading overlay and the error / warning / success alerts                                                  |
| `SelectTable`              | the row picker; slots supply columns beyond the label                                                     |
| `InputRow`                 | a label plus a field — `text`, `number`, `checkbox`, `textarea`, `select`; the slot handles anything else |
| `PageHeader` / `SubHeader` | the blue title bar and the grey "Editing:" bar                                                            |
| `EditorActions`            | Create / Update / Delete, with a slot for extras like Test                                                |

`InputRow` emits a real number for `type="number"`; without it an `Int!` variable
goes out as a string and the mutation is rejected.

**Styles.** App-wide rules live in `assets/main.css`, imported from `main.js` after
Vuetify so they win the cascade. Anything specific to one component belongs in that
component's `<style scoped>`, which is namespaced automatically.

**Vuetify.** `vite-plugin-vuetify` imports only the components actually used in
templates. Do not go back to `import * as components from 'vuetify/components'` —
that registers all 102 and triples the bundle.

**Adding a page.** Create the view, add a lazy route in `router/index.js`, and add
an entry to `navigation.js`. A test asserts the router and the nav agree, so a
missing entry fails the build rather than shipping a dead link.

## Notes

Routing uses hash history (`/#/class`) so the server needs no SPA rewrite rules.

The `Error` route is not in the nav. `main.js` installs an `app.config.errorHandler`
that stashes the stack in `localStorage` and redirects there. Unhandled errors are
deliberately loud — code assumes agreed-upon data is present rather than defending
against its absence. The one exception is `useEditor`'s `run()`, which catches into
the error alert, because everything it catches is a user error the user can correct.

ESLint and Prettier run in IntelliJ on save via `.idea/jsLinters/eslint.xml` and
`.idea/prettier.xml`. Both use the packages in `node_modules` and the config files
here, so the dev dependencies are required even though the IDE appears to provide
them.
