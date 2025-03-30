// Fonction pour envoyer une requête GET et afficher le résultat dans la page
function fetchData(url, outputElement) {
    fetch('http://127.0.0.1:8080'+url)
        .then(response => response.json())
        .then(data => {
            // Afficher les résultats sous forme de liste
            outputElement.innerHTML = data.map(item => {
                const values = Object.values(item);

                // Concaténer les valeurs en une chaîne
                const paragraphe = values.join(' | ');

                return `<p>${paragraphe}</p>`;
            }).join('');
        })
        .catch(error => {
            outputElement.innerHTML = 'Erreur lors de la récupération des données';
            console.error('Erreur:', error);
        });
}


// Sélectionner l'élément de sortie
const outputElement = document.getElementById('output');

const userSpecified = document.getElementById('specific-user');

// Afficher tous les utilisateurs
document.getElementById('show-users').addEventListener('click', () => {
    fetchData('/users', outputElement);
});

// Afficher les utilisateurs suivis par un utilisateur spécifique
document.getElementById('show-following').addEventListener('click', () => {
    const username = userSpecified.value.trim();
    if (username) {
        fetchData(`/following/${username}`, outputElement);
    }
});

// Afficher les utilisateurs qui suivent un utilisateur spécifique
document.getElementById('show-followers').addEventListener('click', () => {
    const username = userSpecified.value.trim();
    if (username) {
        fetchData(`/followers/${username}`, outputElement);
    }
});

// Afficher tous les shouts
document.getElementById('show-shouts').addEventListener('click', () => {
    fetchData('/shouts', outputElement);
});

// Afficher les shouts d'un utilisateur spécifique
document.getElementById('show-user-shouts').addEventListener('click', () => {
    const username = userSpecified.value.trim();
    if (username) {
        fetchData(`/shouts/${username}`, outputElement);
    }
});
