<script setup>
import {links, groups} from '@/navigation'
</script>

<template>
  <div class="home-bg">
    <div class="position-absolute bg-transparent">
      <v-card width="400" class="ma-6 rounded-xl text-white bg-transparent" variant="flat">
        <div class="pa-2" style="background-color: #000000cc; border-bottom: solid 1px #999999">
          <v-card-title class="text-h6">TVA VERIFICATION</v-card-title>
          <v-card-subtitle>Forecast Analytics System</v-card-subtitle>
        </div>
        <div class="pa-6" style="background-color: #000000a0">
          <ul class="ml-4 mb-4">
            <li v-for="l in links" :key="l.to">
              <router-link :to="l.to"><span class="mdi" :class="l.icon" />{{ l.label }}</router-link>
            </li>
          </ul>
          <template v-for="g in groups" :key="g.label">
            <div><span class="mdi mr-2" :class="g.icon" />{{ g.label }}</div>
            <ul class="ml-4 mb-4">
              <li v-for="i in g.items" :key="i.to">
                <router-link :to="i.to"><span class="mdi mdi-link" />{{ i.label }}</router-link>
              </li>
            </ul>
          </template>
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

a:hover {
  color: #dddddd;
  span {
    color: orange;
  }
}

ul {
  list-style-type: none;
  padding-left: 0;
}

.home-bg {
  background: url('@/assets/background2.jpg') top left / cover no-repeat fixed;
  height: 100%;
}
</style>
