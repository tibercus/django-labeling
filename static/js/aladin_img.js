var js_var = JSON.parse(document.getElementById('opt_survey').textContent);

var aladin = A.aladin('#aladin-lite-div', {survey: "P/DSS2/color", fov:1.5, target: "M 20"});

<!-- Change Survey -->
$('input[name=survey]').change(function() {
    aladin.setImageSurvey($(this).val());
});