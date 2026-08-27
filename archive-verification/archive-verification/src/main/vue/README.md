# Verification UI

The browser front end for the FEWS verification service. It is a Vue 3 single-page
app that is **built into the Spring Boot WAR** rather than deployed separately.

## How it fits the Maven build

`pom.xml` runs this project during `generate-resources` via `exec-maven-plugin`:

```
npm install      ->  restores node_modules
npm run build    ->  emits into ../resources/static
```

So `mvn package` produces one WAR containing both the API and this UI. There is no
separate front-end deployment, and no API base URL to configure: the app is served
from the same origin as the API, so `fetch('./graphql')` resolves correctly wherever
the WAR is mounted. That is what `base: "./"` in `vite.config.js` protects — do not
change it to an absolute path.

Because the built output goes to `../resources/static`, that directory is generated,
gitignored, and should never be edited by hand.

## Running it

| Command | Does |
| --- | --- |
| `npm run dev` | Vite dev server with hot reload |
| `npm run build` | Production build into `../resources/static` |
| `npm run build:debug` | Same, unminified and with sourcemaps |
| `npm run preview` | Serve the built output locally |
| `npm test` | Vitest suite |
| `npm run lint` | ESLint (`--fix`) |
| `npm run format` | Prettier |

Note that `npm run dev` has no backend behind it — nothing answers `/graphql`, so
pages will show a connection error. To exercise the UI against real data, build the
WAR and run the Spring Boot app, which serves the UI and the API together.

## Layout

```
src/
  main.js            app entry: router, Vuetify, global CSS
  App.vue            shell - app bar, nav drawer, clock
  graphql.js         the entire data layer (one fetch call)
  navigation.js      the nav tree, shared by App.vue and HomePage.vue
  router/index.js    routes, each a lazy import
  components/        shared UI
  views/             one component per route
  assets/main.css    app-wide styles
```

## Conventions

**Data access.** Every request goes through `graphql(query, variables)` in
`graphql.js`. It POSTs to `./graphql`, throws on a non-2xx response or a GraphQL
`errors[]`, and returns the `data` payload. There is no client cache: pages refetch
after a mutation. Queries are plain template strings held in `const`s at the top of
each view.

**Page shape.** Editor views follow one pattern — a `run()` helper that owns
`loading`/`error`/`success`, called with no argument to reload and with a function
to mutate-then-reload. Copy an existing view rather than inventing a new shape.

**Components.** `StatusBar` renders the loading overlay and result alerts.
`InputRow` renders a label plus a field; it emits a real number for
`type="number"`, and its default slot replaces the input for selects, textareas,
checkboxes and JSON editors.

**Styles.** App-wide rules live in `assets/main.css`, imported once from `main.js`
after Vuetify so they win the cascade. Anything specific to one component belongs in
that component's `<style scoped>`.

**Adding a page.** Create the view, add a lazy route in `router/index.js`, and add
an entry to `navigation.js` — a test asserts the router and the nav agree.

## Notes

Routing uses hash history (`/#/class`) so the server needs no SPA rewrite rules.

The `Error` route is not in the nav; `main.js` installs an `app.config.errorHandler`
that stashes the stack in `localStorage` and redirects there. Unhandled errors are
deliberately loud — code assumes agreed-upon data is present rather than defending
against its absence.
