<script setup>

</script>

<template>
  <div class="home-bg">
    <div class="position-absolute bg-transparent">
      <v-card width="400" class="ma-6 rounded-xl text-white bg-transparent" variant="flat">
        <div class="pa-2" style="background-color: #000000cc; border-bottom: solid 1px #999999;">
          <v-card-title class="text-h6">TVA VERIFICATION</v-card-title>
          <v-card-subtitle>Forecast Analytics System</v-card-subtitle>
        </div>
        <div class="pa-6" style="background-color: #000000a0;">
          <ul class="ml-4 mb-4">
            <li><router-link to="/"><span class="mdi mdi-home-outline"/>Home</router-link></li>
            <li><router-link to="/configurationSettings"><span class="mdi mdi-cog-outline"/>Settings</router-link></li>
          </ul>
          <div><span class="mdi mdi-check-all mr-2"/>Verification</div>
          <ul class="ml-4 mb-4">
            <li><router-link to="/class"><span class="mdi mdi-link"/>Class</router-link></li>
            <li><router-link to="/forecast"><span class="mdi mdi-link"/>Forecast</router-link></li>
            <li><router-link to="/locationAttributes"><span class="mdi mdi-link"/>LocationAttributes</router-link></li>
            <li><router-link to="/normal"><span class="mdi mdi-link"/>Normal</router-link></li>
            <li><router-link to="/observed"><span class="mdi mdi-link"/>Observed</router-link></li>
            <li><router-link to="/seasonality"><span class="mdi mdi-link"/>Seasonality</router-link></li>
            <li><router-link to="/study"><span class="mdi mdi-link"/>Study</router-link></li>
          </ul>
          <div><span class="mdi mdi-waves mr-2"/>Fews</div>
          <ul class="ml-4 mb-4">
            <li><router-link to="/fewsLocations"><span class="mdi mdi-link"/>Locations</router-link></li>
            <li><router-link to="/fewsParameters"><span class="mdi mdi-link"/>Parameters</router-link></li>
            <li><router-link to="/fewsQualifiers"><span class="mdi mdi-link"/>Qualifiers</router-link></li>
          </ul>
          <div><span class="mdi mdi-export mr-2"/>Output</div>
          <ul class="ml-4 mb-4">
            <li><router-link to="/outputCube"><span class="mdi mdi-link"/>Cube</router-link></li>
            <li><router-link to="/outputPowerQuery"><span class="mdi mdi-link"/>PowerQuery</router-link></li>
          </ul>
          <div><span class="mdi mdi-artboard mr-2"/>Template</div>
          <ul class="ml-4 mb-4">
            <li><router-link to="/templateCube"><span class="mdi mdi-link"/>Cube</router-link></li>
            <li><router-link to="/templateDrdlYaml"><span class="mdi mdi-link"/>DrdlYaml</router-link></li>
            <li><router-link to="/templatePowerQuery"><span class="mdi mdi-link"/>PowerQuery</router-link></li>
          </ul>
        </div>
      </v-card>
    </div>
  </div>
</template>

<style scoped>
ul span {
  margin-right: 10px;
  width: 20px;
}

a {
  color: white;
  text-decoration: none;
}

a:hover{
  color: #dddddd;
  span{
    color:orange;
  }
}

ul {
  list-style-type: none;
  padding-left: 0;
}

.home-bg{
  background: url('@/assets/background2.jpg') top left / cover no-repeat fixed;
  height: 100%;
}
</style>