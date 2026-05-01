/**
 * workbench.js — form enhancement and response rendering.
 */

/* Copy response text to clipboard */
function copyResponse(elementId) {
    var el = document.getElementById(elementId);
    if (el) {
        navigator.clipboard.writeText(el.textContent);
    }
}

/* Toggle between raw and rendered view */
function toggleView(rawId, renderedId) {
    var raw = document.getElementById(rawId);
    var rendered = document.getElementById(renderedId);
    if (raw) raw.classList.toggle('hidden');
    if (rendered) rendered.classList.toggle('hidden');
}

/* Quick fill a form field */
function fillField(fieldName, value) {
    var input = document.querySelector('[name="' + fieldName + '"]');
    if (input) input.value = value;
}
