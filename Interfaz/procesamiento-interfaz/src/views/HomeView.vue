<template>
  <div class="home">
    <Graph/>
  </div>
</template>

<script>
// @ is an alias to /src
import Graph from '@/components/Graph.vue'

export default {
  name: 'HomeView',
  components: {
    Graph
  }
}
</script>
