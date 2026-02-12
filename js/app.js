// Main Application Logic
document.addEventListener('DOMContentLoaded', () => {
    console.log('App initialized');

    // Setup active link in sidebar based on current URL
    const currentPath = window.location.pathname.split('/').pop();
    const navLinks = document.querySelectorAll('.nav-link');
    
    navLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href === currentPath || (currentPath === '' && href === 'index.html')) {
            link.classList.add('active');
        }
    });
});

// Helper functions for UI interactions
function toggleModal(modalId) {
    const modal = document.getElementById(modalId);
    if (modal) {
        modal.classList.toggle('active');
    }
}
