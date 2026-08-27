import {createRouter, createWebHashHistory} from 'vue-router'

export default createRouter({
  history: createWebHashHistory(),
  routes: [
    {path: '/', name: 'Home', component: () => import('../views/HomePage.vue')},
    {path: '/error', name: 'Error', component: () => import('../views/ErrorPage.vue')},
    {path: '/class', name: 'Class', component: () => import('../views/ClassPage.vue')},
    {path: '/configurationDescription', name: 'ConfigurationDescription', component: () => import('../views/ConfigurationDescriptionPage.vue')},
    {path: '/configurationSettings', name: 'ConfigurationSettings', component: () => import('../views/ConfigurationSettingsPage.vue')},
    {path: '/dimensionIsOriginalForecast', name: 'DimensionIsOriginalForecast', component: () => import('../views/DimensionIsOriginalForecastPage.vue')},
    {path: '/dimensionIsOriginalObserved', name: 'DimensionIsOriginalObserved', component: () => import('../views/DimensionIsOriginalObservedPage.vue')},
    {path: '/dimensionMeasure', name: 'DimensionMeasure', component: () => import('../views/DimensionMeasurePage.vue')},
    {path: '/fewsLocations', name: 'FewsLocations', component: () => import('../views/FewsLocationsPage.vue')},
    {path: '/fewsParameters', name: 'FewsParameters', component: () => import('../views/FewsParametersPage.vue')},
    {path: '/fewsQualifiers', name: 'FewsQualifiers', component: () => import('../views/FewsQualifiersPage.vue')},
    {path: '/forecast', name: 'Forecast', component: () => import('../views/ForecastPage.vue')},
    {path: '/locationAttributes', name: 'LocationAttributes', component: () => import('../views/LocationAttributesPage.vue')},
    {path: '/normal', name: 'Normal', component: () => import('../views/NormalPage.vue')},
    {path: '/observed', name: 'Observed', component: () => import('../views/ObservedPage.vue')},
    {path: '/outputPowerQuery', name: 'OutputPowerQuery', component: () => import('../views/OutputPowerQueryPage.vue')},
    {path: '/seasonality', name: 'Seasonality', component: () => import('../views/SeasonalityPage.vue')},
    {path: '/study', name: 'Study', component: () => import('../views/StudyPage.vue')},
    {path: '/templateCube', name: 'TemplateCube', component: () => import('../views/TemplateCubePage.vue')},
    {path: '/templateDrdlYaml', name: 'TemplateDrdlYaml', component: () => import('../views/TemplateDrdlYamlPage.vue')},
    {path: '/templatePowerQuery', name: 'TemplatePowerQuery', component: () => import('../views/TemplatePowerQueryPage.vue')},
  ],
})
