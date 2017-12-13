var Service = require('node-windows').Service;

// Create a new service object
var svc = new Service({
  name:'EDDN Exploration Data Collector',
  description: 'Collects Body and Stellar Data',
  script: 'C:\\Users\\Kelsey\\Development\\eddn_body_collector\\index.js',
  nodeOptions: [
    '--harmony',
    '--max_old_space_size=4096'
  ]
});

// Listen for the "install" event, which indicates the
// process is available as a service.
svc.on('install',function(){
  svc.start();
});

svc.install();