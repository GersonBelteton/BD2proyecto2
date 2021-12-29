var assert = require('assert');
//”cassandra-driver” is in the node_modules folder. Redirect if necessary.
var cassandra = require('cassandra-driver');

//Replace Username and Password with your cluster settings
var authProvider = new cassandra.auth.PlainTextAuthProvider('cassandra', 'cassandra');
var datacenter = 'datacenter1'
//Replace PublicIP with the IP addresses of your clusters
var contactPoints = ['localhost'];
var client = new cassandra.Client({ contactPoints: contactPoints, keyspace: 'bd2proyecto2', localDataCenter: datacenter });





function consulta5(rows) {
    var query = 'Select count(idPaciente) as cantidad, edad from paciente_e group by edad;';
    let params = [];
    client.execute(query, params, { prepare: true }, (err, result) => {
        if (err) {
            console.log(err)
        } else {
            let arreglo = []
            result.rows.forEach(element => {
                arreglo.push({ edad: element.edad.toString(), cantidad: element.cantidad.toNumber() })
            });
            var ar = arreglo.sort(function (a, b) { return parseFloat(b.cantidad) - parseFloat(a.cantidad) }).slice(0, 5)
            console.table(ar)
        }
    });

}

consulta5();
function consulta6(rows) {
    var query = 'Select count(idPaciente) as cantidad,idHabitacion, habitacion from logactividad group by idHabitacion;';
    let params = [];
    client.execute(query, params, { prepare: true }, (err, result) => {
        if (err) {
            console.log(err)
        } else {
            let arreglo = []
            result.rows.forEach(element => {
                arreglo.push({ habitacion: element.habitacion.toString(), cantidad: element.cantidad.toNumber() })
            });
            var ar = arreglo.sort(function (a, b) { return parseFloat(b.cantidad) - parseFloat(a.cantidad) }).slice(0, 5)
            console.table(ar)
        }
    });

}
//consulta6()

//=================================================

function consulta8() {
    var query = 'Select idHabitacion, habitacion, count(status) as cantidad from loghabitacion where status like \'%Inicia limpieza%\' group by idHabitacion;'
    let params = []
    client.execute(query, params, { prepare: true }, (err, result) => {
        if (err) {
            console.log(err)
        } else {
            let arreglo = []
            result.rows.forEach(element => {
                arreglo.push({ habitacion: element.habitacion.toString(), cantidad: element.cantidad.toNumber() })
            });
            var ar = arreglo.sort(function (a, b) { return parseFloat(b.cantidad) - parseFloat(a.cantidad) }).slice(0, 5)
            console.table(ar)
        }
    })
}


//consulta8()

//=====================================================

function consulta10() {
    var query = 'Select date, count(idPaciente) as cantidad from fechapaciente group by date;'
    let params = []
    client.execute(query, params, { prepare: true }, (err, result) => {
        if (err) {
            console.log(err)
        } else {
            let arreglo = []
            result.rows.forEach(element => {
                arreglo.push({ date: element.date.toString(), cantidad: element.cantidad.toNumber() })
            });
            var ar = arreglo.sort(function (a, b) { return parseFloat(b.cantidad) - parseFloat(a.cantidad) }).slice(0, 1)
            console.table(ar)
        }
    })
}

consulta10()