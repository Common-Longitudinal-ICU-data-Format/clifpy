// Force dark mode as default
document.addEventListener('DOMContentLoaded', function() {
  // Set dark mode if no preference is stored
  if (!localStorage.getItem('data-md-color-scheme')) {
    document.documentElement.setAttribute('data-md-color-scheme', 'slate');
    localStorage.setItem('data-md-color-scheme', 'slate');
  }
});