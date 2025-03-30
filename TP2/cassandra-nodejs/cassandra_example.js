const express = require('express');
const cassandra = require('cassandra-driver');
const cors = require('cors');

const app = express();
const port = 8080;


// Configuration du cluster
const client = new cassandra.Client({
    contactPoints: ['127.0.0.1:32769'],
    localDataCenter: 'datacenter1', // Remplacez par votre nom de datacenter si différent
    keyspace: 'shoutter',
});

// Fonction de connexion à Cassandra
client.connect()
    .then(() => console.log('Connexion à Cassandra effectuée avec succès !'))
    .catch(err => console.error('Erreur lors de la connexion à Cassandra:', err));

app.use(cors());
app.use(express.json());

// Route pour récupérer tous les utilisateurs
app.get('/users', (req, res) => {
    const query = 'SELECT * FROM USERS';
    client.execute(query, [], (err, result) => {
        if (err) {
            res.status(500).json({ error: 'Erreur de la base de données' });
            return;
        }
        res.json(result.rows); // Retourne les résultats en JSON
    });
});

// Route pour récupérer tous les utilisateurs suivis par un utilisateur spécifique
app.get('/following/:username', (req, res) => {
    const { username } = req.params;
    const query = 'SELECT followed FROM FOLLOWING WHERE username = ?';
    client.execute(query, [username], (err, result) => {
        if (err) {
            res.status(500).json({ error: 'Erreur de la base de données' });
            return;
        }
        res.json(result.rows); // Retourne les résultats en JSON
    });
});

// Route pour récupérer tous les utilisateurs qui suivent un utilisateur spécifique
app.get('/followers/:username', (req, res) => {
    const { username } = req.params;
    const query = 'SELECT following FROM FOLLOWERS WHERE username = ?';
    client.execute(query, [username], (err, result) => {
        if (err) {
            res.status(500).json({ error: 'Erreur de la base de données' });
            return;
        }
        res.json(result.rows); // Retourne les résultats en JSON
    });
});

// Route pour récupérer tous les "shouts"
app.get('/shouts', (req, res) => {
    const query = 'SELECT username, body FROM SHOUTS';
    client.execute(query, [], (err, result) => {
        if (err) {
            res.status(500).json({ error: 'Erreur de la base de données' });
            return;
        }
        res.json(result.rows); // Retourne les résultats en JSON
    });
});

// Route pour récupérer tous les "shouts" d'un utilisateur spécifique
app.get('/shouts/:username', (req, res) => {
    const { username } = req.params;
    const query = 'SELECT body FROM USERSHOUTS WHERE username = ?';
    client.execute(query, [username], (err, result) => {
        if (err) {
            res.status(500).json({ error: 'Erreur de la base de données' });
            return;
        }
        res.json(result.rows); // Retourne les résultats en JSON
    });
});






// Lancement du serveur
app.listen(port, () => {
    console.log('Serveur en cours d\'exécution\nA l\'adresse: localhost\nAu port: ' + port);
});