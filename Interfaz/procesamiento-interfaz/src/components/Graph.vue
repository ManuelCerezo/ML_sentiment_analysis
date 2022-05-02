<template>
  <h1>Esto es una grafica</h1>
</template>

<script>
import axios from "axios";
export default {
    name:'Graph',
    data(){
        return{
            interval:null
        }
    },methods:{
        refreshdata:function(){
            this.interval = setInterval(()=>{
                axios.get("http://localhost:5000/puerta-enlace/getdatos")
                .then((response) => {
                    console.log(response.data)
                })
                .catch((error) => console.log(error));

            },2000)
        }
    },mounted(){
        this.refreshdata()
    }
       
}
</script>

<style>

</style>