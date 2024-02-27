<script setup>

</script>

<template>
  <div class="home-bg">
    <div class="position-absolute bg-transparent">
      <v-card width="400" class="ma-6 rounded-xl text-white bg-transparent" variant="flat">
        <div class="pa-2" style="background-color: #000000cc; border-bottom: solid 1px #999999;">
          <v-card-title class="text-h5">TVA VERIFICATION</v-card-title>
          <v-card-subtitle class="text-sm-body-1">Forecast Analytics System</v-card-subtitle>
        </div>
        <v-list density="compact" class="pb-6 text-white" style="background-color: #000000a0;">
          <v-list-item prepend-icon="mdi-home-outline" to="/">Home</v-list-item>
          <v-list-item style="background-color: #00000032" prepend-icon="mdi-check-all">Verification</v-list-item>
          <div class="ml-14">
            <v-list-item prepend-icon="mdi-link" to="/class">Class</v-list-item>
            <v-list-item prepend-icon="mdi-link" to="/forecast">Forecast</v-list-item>
            <v-list-item prepend-icon="mdi-link" to="/locationAttributes">LocationAttributes</v-list-item>
            <v-list-item prepend-icon="mdi-link" to="/normal">Normal</v-list-item>
            <v-list-item prepend-icon="mdi-link" to="/observed">Observed</v-list-item>
            <v-list-item prepend-icon="mdi-link" to="/seasonality">Seasonality</v-list-item>
            <v-list-item prepend-icon="mdi-link" to="/study">Study</v-list-item>
          </div>
          <v-list-item style="background-color: #00000032" prepend-icon="mdi-waves">Fews</v-list-item>
          <div class="ml-14">
            <v-list-item prepend-icon="mdi-link" to="/fewsLocations">Locations</v-list-item>
            <v-list-item prepend-icon="mdi-link" to="/fewsParameters">Parameters</v-list-item>
            <v-list-item prepend-icon="mdi-link" to="/fewsQualifiers">Qualifiers</v-list-item>
          </div>
          <v-list-item style="background-color: #00000032" prepend-icon="mdi-export">Output</v-list-item>
          <div class="ml-14">
            <v-list-item prepend-icon="mdi-link" to="/outputCube">Cube</v-list-item>
            <v-list-item prepend-icon="mdi-link" to="/outputPowerQuery">PowerQuery</v-list-item>
          </div>
          <v-list-item style="background-color: #00000032" prepend-icon="mdi-artboard">Template</v-list-item>
          <div class="ml-14">
            <v-list-item prepend-icon="mdi-link" to="/templateCube">Cube</v-list-item>
            <v-list-item prepend-icon="mdi-link" to="/templateDrdlYaml">DrdlYaml</v-list-item>
            <v-list-item prepend-icon="mdi-link" to="/templatePowerQuery">PowerQuery</v-list-item>
          </div>
          <v-list-item prepend-icon="mdi-cog-outline" to="/configurationSettings">Settings</v-list-item>
        </v-list>
      </v-card>
    </div>
  </div>
</template>

<style scoped>
.v-list div{
  padding: 5px 15px 15px;
}
.v-list div .v-list-item{
  min-height: unset;
  padding: 0;
}
.home-bg{
  background: url('@/assets/background2.jpg') top left / cover no-repeat fixed;
  height: 100%;
}
</style>