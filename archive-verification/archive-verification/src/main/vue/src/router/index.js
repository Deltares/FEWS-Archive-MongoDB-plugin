import {createRouter, createWebHashHistory} from "vue-router";

import HomePage from '../views/HomePage.vue';
import ClassPage from '../views/ClassPage.vue';
import ConfigurationSettingsPage from '../views/ConfigurationSettingsPage.vue';
import ConfigurationDescriptionPage from '../views/ConfigurationDescriptionPage.vue';
import FewsLocationsPage from '../views/FewsLocationsPage.vue';
import FewsParametersPage from '../views/FewsParametersPage.vue';
import FewsQualifiersPage from '../views/FewsQualifiersPage.vue';
import ForecastPage from '../views/ForecastPage.vue';
import LocationAttributesPage from '../views/LocationAttributesPage.vue';
import NormalPage from '../views/NormalPage.vue';
import ObservedPage from '../views/ObservedPage.vue';
import OutputCubePage from '../views/OutputCubePage.vue';
import OutputPowerQueryPage from '../views/OutputPowerQueryPage.vue';
import SeasonalityPage from '../views/SeasonalityPage.vue';
import StudyPage from '../views/StudyPage.vue';
import TemplateCubePage from '../views/TemplateCubePage.vue';
import TemplateDrdlYamlPage from '../views/TemplateDrdlYamlPage.vue';
import TemplatePowerQueryPage from '../views/TemplatePowerQueryPage.vue';

export default createRouter({
    history: createWebHashHistory(),
    routes:[
        {path: '/', name: "Home", component: HomePage},
        {path: '/class', name: "Class", component: ClassPage},
        {path: '/configurationSettings', name: "ConfigurationSettings", component: ConfigurationSettingsPage},
        {path: '/configurationDescription', name: "ConfigurationDescription", component: ConfigurationDescriptionPage},
        {path: '/fewsLocations', name: "FewsLocations", component: FewsLocationsPage},
        {path: '/fewsParameters', name: "FewsParameters", component: FewsParametersPage},
        {path: '/fewsQualifiers', name: "FewsQualifiers", component: FewsQualifiersPage},
        {path: '/forecast', name: "Forecast", component: ForecastPage},
        {path: '/locationAttributes', name: "LocationAttributes", component: LocationAttributesPage},
        {path: '/normal', name: "Normal", component: NormalPage},
        {path: '/observed', name: "Observed", component: ObservedPage},
        {path: '/outputCube', name: "OutputCube", component: OutputCubePage},
        {path: '/outputPowerQuery', name: "OutputPowerQuery", component: OutputPowerQueryPage},
        {path: '/seasonality', name: "Seasonality", component: SeasonalityPage},
        {path: '/study', name: "Study", component: StudyPage},
        {path: '/templateCube', name: "TemplateCube", component: TemplateCubePage},
        {path: '/templateDrdlYaml', name: "TemplateDrdlYaml", component: TemplateDrdlYamlPage},
        {path: '/templatePowerQuery', name: "TemplatePowerQuery", component: TemplatePowerQueryPage}
    ]
});
