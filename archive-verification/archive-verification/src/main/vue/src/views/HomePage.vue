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
            <li><a href="/"><span class="mdi mdi-home-outline"/>Home</a></li>
            <li><a href="/configurationSettings"><span class="mdi mdi-cog-outline"/>Settings</a></li>
          </ul>
          <div><span class="mdi mdi-check-all mr-2"/>Verification</div>
          <ul class="ml-4 mb-4">
            <li><a href="/class"><span class="mdi mdi-link"/>Class</a></li>
            <li><a href="/forecast"><span class="mdi mdi-link"/>Forecast</a></li>
            <li><a href="/locationAttributes"><span class="mdi mdi-link"/>LocationAttributes</a></li>
            <li><a href="/normal"><span class="mdi mdi-link"/>Normal</a></li>
            <li><a href="/observed"><span class="mdi mdi-link"/>Observed</a></li>
            <li><a href="/seasonality"><span class="mdi mdi-link"/>Seasonality</a></li>
            <li><a href="/study"><span class="mdi mdi-link"/>Study</a></li>
          </ul>
          <div><span class="mdi mdi-waves mr-2"/>Fews</div>
          <ul class="ml-4 mb-4">
            <li><a href="/fewsLocations"><span class="mdi mdi-link"/>Locations</a></li>
            <li><a href="/fewsParameters"><span class="mdi mdi-link"/>Parameters</a></li>
            <li><a href="/fewsQualifiers"><span class="mdi mdi-link"/>Qualifiers</a></li>
          </ul>
          <div><span class="mdi mdi-export mr-2"/>Output</div>
          <ul class="ml-4 mb-4">
            <li><a href="/outputCube"><span class="mdi mdi-link"/>Cube</a></li>
            <li><a href="/outputPowerQuery"><span class="mdi mdi-link"/>PowerQuery</a></li>
          </ul>
          <div><span class="mdi mdi-artboard mr-2"/>Template</div>
          <ul class="ml-4 mb-4">
            <li><a href="/templateCube"><span class="mdi mdi-link"/>Cube</a></li>
            <li><a href="/templateDrdlYaml"><span class="mdi mdi-link"/>DrdlYaml</a></li>
            <li><a href="/templatePowerQuery"><span class="mdi mdi-link"/>PowerQuery</a></li>
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