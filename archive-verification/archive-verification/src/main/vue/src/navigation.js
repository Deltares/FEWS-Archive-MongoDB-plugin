export const links = [
  {icon: 'mdi-home-outline', label: 'Home', to: '/'},
  {icon: 'mdi-cog-outline', label: 'Settings', to: '/configurationSettings'},
  {icon: 'mdi-cogs', label: 'Description', to: '/configurationDescription'},
]

export const groups = [
  {
    icon: 'mdi-check-all',
    label: 'Verification',
    items: [
      {label: 'Class', to: '/class'},
      {label: 'Forecast', to: '/forecast'},
      {label: 'LocationAttributes', to: '/locationAttributes'},
      {label: 'Normal', to: '/normal'},
      {label: 'Observed', to: '/observed'},
      {label: 'Seasonality', to: '/seasonality'},
      {label: 'Study', to: '/study'},
    ],
  },
  {
    icon: 'mdi-waves',
    label: 'Fews',
    items: [
      {label: 'Locations', to: '/fewsLocations'},
      {label: 'Parameters', to: '/fewsParameters'},
      {label: 'Qualifiers', to: '/fewsQualifiers'},
    ],
  },
  {
    icon: 'mdi-export',
    label: 'Output',
    items: [{label: 'PowerQuery', to: '/outputPowerQuery'}],
  },
  {
    icon: 'mdi-artboard',
    label: 'Template',
    items: [
      {label: 'Cube', to: '/templateCube'},
      {label: 'DrdlYaml', to: '/templateDrdlYaml'},
      {label: 'PowerQuery', to: '/templatePowerQuery'},
    ],
  },
  {
    icon: 'mdi-axis-arrow',
    label: 'Dimension',
    items: [
      {label: 'IsOriginalForecast', to: '/dimensionIsOriginalForecast'},
      {label: 'IsOriginalObserved', to: '/dimensionIsOriginalObserved'},
      {label: 'Measure', to: '/dimensionMeasure'},
    ],
  },
]
