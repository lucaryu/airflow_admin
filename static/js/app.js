// Main Application Logic
document.addEventListener('DOMContentLoaded', () => {
    console.log('App initialized');

    // Load Template Variables immediately
    loadTemplateVariables();

    // Setup active link in sidebar based on current URL
    const currentPath = window.location.pathname.split('/').pop();
    const navLinks = document.querySelectorAll('.nav-link');

    navLinks.forEach(link => {
        const href = link.getAttribute('href');
        // Simple matching logic
        if (href === currentPath || (currentPath === '' && href === 'index.html')) {
            link.classList.add('active');
        }
    });

    // Handle "Register New Template" button click
    const btnRegistTemplate = document.getElementById('btn-regist-template');
    if (btnRegistTemplate) {
        btnRegistTemplate.addEventListener('click', () => {
            // Clear form fields
            document.getElementById('template-id').value = '';
            document.getElementById('template-name').value = '';
            document.getElementById('source-type').selectedIndex = 0;
            document.getElementById('target-type').selectedIndex = 0;
            document.getElementById('template-comment').value = '';
            document.getElementById('template-code').value = '';

            // Reset button states
            document.getElementById('btn-save-template').style.display = 'inline-block';
            document.getElementById('btn-update-template').style.display = 'none';
            document.getElementById('btn-delete-template').style.display = 'none';

            // Focus on the name field
            document.getElementById('template-name').focus();
        });
    }

    // Handle Template List Item Click
    const templateList = document.querySelector('.template-list');
    if (templateList) {
        templateList.addEventListener('click', (e) => {
            const item = e.target.closest('.template-item');
            if (item) {
                const id = item.getAttribute('data-id');

                // Fetch template details
                fetch(`/api/templates/${id}`)
                    .then(response => response.json())
                    .then(data => {
                        // Populate form
                        document.getElementById('template-id').value = data.id;
                        document.getElementById('template-name').value = data.name;
                        document.getElementById('source-type').value = data.source_type;
                        document.getElementById('target-type').value = data.target_type;
                        document.getElementById('template-comment').value = data.comment || '';
                        document.getElementById('template-code').value = data.code;
                        if (typeof codeMirrorEditor !== 'undefined' && codeMirrorEditor) {
                            codeMirrorEditor.setValue(data.code);
                        }

                        // Show update/delete buttonsem
                        document.querySelectorAll('.template-item').forEach(el => {
                            el.style.backgroundColor = '';
                            el.style.borderLeft = '';
                            el.style.fontWeight = 'normal';
                        });
                        item.style.backgroundColor = 'rgba(25, 127, 230, 0.1)';
                        item.style.borderLeft = '3px solid var(--primary-color)';
                        item.style.fontWeight = '600';

                        // Show Update/Delete buttons, hide Save button
                        document.getElementById('btn-save-template').style.display = 'none';
                        document.getElementById('btn-update-template').style.display = 'inline-block';
                        document.getElementById('btn-delete-template').style.display = 'inline-block';
                    })
                    .catch(error => console.error('Error fetching template:', error));
            }
        });
    }

    // Handle "Save Template" button click
    const btnSaveTemplate = document.getElementById('btn-save-template');
    if (btnSaveTemplate) {
        btnSaveTemplate.addEventListener('click', () => {
            const name = document.getElementById('template-name').value;
            const sourceType = document.getElementById('source-type').value;
            const targetType = document.getElementById('target-type').value;
            const comment = document.getElementById('template-comment').value;
            const code = document.getElementById('template-code').value;

            if (!name || !code) {
                alert('Template Name and Code are required.');
                return;
            }

            fetch('/api/templates', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    name: name,
                    source_type: sourceType,
                    target_type: targetType,
                    comment: comment,
                    code: code
                })
            })
                .then(response => response.json())
                .then(data => {
                    if (data.status === 'success') {
                        alert('Template saved successfully!');
                        location.reload();
                    } else {
                        alert('Error saving template: ' + data.message);
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                    alert('An error occurred while saving the template.');
                });
        });
    }

    // Handle "Update Template" button click
    const btnUpdateTemplate = document.getElementById('btn-update-template');
    if (btnUpdateTemplate) {
        btnUpdateTemplate.addEventListener('click', () => {
            const id = document.getElementById('template-id').value;
            if (!id) return;

            const name = document.getElementById('template-name').value;
            const sourceType = document.getElementById('source-type').value;
            const targetType = document.getElementById('target-type').value;
            const comment = document.getElementById('template-comment').value;
            const code = document.getElementById('template-code').value;

            if (!name || !code) {
                alert('Template Name and Code are required.');
                return;
            }

            fetch(`/api/templates/${id}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    name: name,
                    source_type: sourceType,
                    target_type: targetType,
                    comment: comment,
                    code: code
                })
            })
                .then(response => response.json())
                .then(data => {
                    if (data.status === 'success') {
                        alert('Template updated successfully!');
                        location.reload();
                    } else {
                        alert('Error updating template: ' + data.message);
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                    alert('An error occurred while updating the template.');
                });
        });
    }

    // Handle "Delete Template" button click
    const btnDeleteTemplate = document.getElementById('btn-delete-template');
    if (btnDeleteTemplate) {
        btnDeleteTemplate.addEventListener('click', () => {
            const id = document.getElementById('template-id').value;
            if (!id) return;

            if (!confirm('Are you sure you want to delete this template?')) return;

            fetch(`/api/templates/${id}`, {
                method: 'DELETE'
            })
                .then(response => response.json())
                .then(data => {
                    if (data.status === 'success') {
                        alert('Template deleted successfully!');
                        location.reload();
                    } else {
                        alert('Error deleting template: ' + data.message);
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                    alert('An error occurred while deleting the template.');
                });
        });
    }



    // Handle "Add Variable" button
    const btnAddVariable = document.getElementById('btn-add-variable');
    if (btnAddVariable) {
        btnAddVariable.addEventListener('click', () => {
            toggleModal('variable-modal');
        });
    }



}); // End of DOMContentLoaded

function loadTemplateVariables() {
    fetch('/api/template-variables')
        .then(response => response.json())
        .then(variables => {
            const toolbar = document.getElementById('variable-toolbar');
            const sidebarList = document.getElementById('sidebar-variable-list');

            // Try to find addBtn, but don't fail if it's missing
            const addBtn = document.getElementById('btn-add-variable');

            // Update Toolbar
            // Update Toolbar
            if (toolbar) {
                // Remove only dynamic variable buttons
                // Instead of clearing innerHTML, remove elements that are not the static controls
                Array.from(toolbar.children).forEach(child => {
                    const isStatic = child.id === 'btn-add-variable' || child.id === 'btn-fullscreen' || child.tagName === 'SPAN';
                    // Note: 'Loading toolbar...' span is preserved initially but we might want to remove it if variables loaded
                    if (!isStatic) {
                        child.remove();
                    }
                    if (child.tagName === 'SPAN' && child.textContent.includes('Loading')) {
                        child.remove();
                    }
                });

                const fsBtn = document.getElementById('btn-fullscreen');
                const addBtn = document.getElementById('btn-add-variable');

                if (variables.length === 0) {
                    // Optionally show a message, but create it as a distinct element that we can clear later
                    // For now, doing nothing is fine, or adding a text node.
                }

                variables.forEach(v => {
                    const btn = document.createElement('button');
                    btn.type = 'button';
                    // Map color names to CSS vars or styles
                    const color = v.color || 'secondary';
                    if (color.startsWith('#')) {
                        btn.className = 'btn';
                        btn.style.backgroundColor = color;
                        btn.style.color = '#fff';
                    } else {
                        btn.className = `btn btn-${color}`;
                    }
                    btn.style.padding = '4px 8px';
                    btn.style.fontSize = '12px';
                    btn.style.marginRight = '8px';
                    btn.innerHTML = `<i class="${v.icon}"></i> ${v.name}`;

                    btn.addEventListener('click', () => {
                        const editor = document.getElementById('template-code');
                        // Use replaceSelection logic
                        replaceSelection(editor, v.code);
                    });

                    // Context menu for deletion
                    btn.addEventListener('contextmenu', (e) => {
                        e.preventDefault();
                        if (confirm(`Delete variable "${v.name}"?`)) {
                            fetch(`/api/template-variables/${v.id}`, { method: 'DELETE' })
                                .then(() => loadTemplateVariables());
                        }
                    });

                    // Insert logic:
                    // If addBtn exists, insert before it.
                    // Else if fsBtn exists, insert before it.
                    // Else append.
                    // Actually, we want variables -> addBtn -> fsBtn order? Or variables -> fsBtn
                    // Current layout: dynamic buttons ... [fullscreen]

                    if (addBtn) {
                        toolbar.insertBefore(btn, addBtn);
                    } else if (fsBtn) {
                        toolbar.insertBefore(btn, fsBtn);
                    } else {
                        toolbar.appendChild(btn);
                    }
                });
            }

        })
        .catch(err => {
            console.error('Error loading variables:', err);
            const toolbar = document.getElementById('variable-toolbar');
            if (toolbar) toolbar.innerHTML = '<span style="color: var(--error-color);">Error loading variables.</span>';
        });
}

// Helper functions for UI interactions
function toggleModal(modalId) {
    // This is now overridden in HTML script tag but good to keep basic logic or delegate
    if (window.toggleModal && window.toggleModal !== toggleModal) {
        window.toggleModal(modalId);
    } else {
        const modal = document.getElementById(modalId);
        if (modal) {
            modal.classList.toggle('active');
        }
    }
}

function replaceSelection(textarea, replacement) {
    // Check if CodeMirror is active (global variable from dag_template.html)
    if (typeof codeMirrorEditor !== 'undefined' && codeMirrorEditor) {
        const doc = codeMirrorEditor.getDoc();
        const cursor = doc.getCursor();
        doc.replaceRange(replacement, cursor);
        // Move cursor to end of inserted text? replaceRange does not automatically move it unless we calculate
        // But typically we just want to insert.
        // If we want to replace selection:
        // doc.replaceSelection(replacement); // This is better if there is a selection
        return;
    }

    if (!textarea) return;
    console.log('Inserting variable:', replacement);
    const start = textarea.selectionStart;
    const end = textarea.selectionEnd;
    const text = textarea.value;
    const before = text.substring(0, start);
    const after = text.substring(end, text.length);
    textarea.value = before + replacement + after;
    textarea.selectionStart = textarea.selectionEnd = start + replacement.length;
    textarea.focus();
}
