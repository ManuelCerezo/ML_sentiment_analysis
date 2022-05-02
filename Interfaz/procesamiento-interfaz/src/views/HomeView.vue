<template>
  <div class="home">
    <h1>Esto es una grafica</h1>
      <line-chart :chart-data="this.datacollection"></line-chart>
  </div>
</template>

<script>
// @ is an alias to /src
import LineChart from '@/components/LineChart.js'
import axios from "axios";
export default {
  name: 'HomeView',
  components: {
    LineChart
  },data(){
        return{
            label:"hola",
            interval:null,
            vader_analyzer:[],
            process_time:[],
            chartOptions:{
              responsive:true,
              maintainAspectRatio:false
            },
            datacollection:{
              labels:["uno","dos","tres","cuatro","cinco","seis","siete","ocho","nueve","diez"],
              datasets:[
                {
                  data:[1,2,3,4,5,6,7,8,9,10],
                  label:"Africa",
                  borderColor: "#3e95cd",
                  fill: false
                }
              ]
            }
        }
    },methods:{
        refreshdata:function(){
            this.interval = setInterval(()=>{
                axios.get("http://localhost:5000/puerta-enlace/getdatos")
                .then((response) => {
                    console.log(response.data.vader_analyzer_data)
                    this.process_time = response.data.process_time_data
                    this.vader_analyzer = response.data.vader_analyzer_data
                })
                .catch((error) => console.log(error));

            },3000)
        }
  },mounted(){
      this.refreshdata()
  }
}
</script>
