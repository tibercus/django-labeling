var aladin = A.aladin('#aladin-lite-div', {survey: "P/DSS2/color", fov:1.5, target: "M 20"});
<!-- Cjange Survey -->
$('input[name=survey]').change(function() {
    aladin.setImageSurvey($(this).val());
});
<!-- Display SIMBAD and VizieR data -->
aladin.addCatalog(A.catalogFromSimbad('M 20', 0.2, {shape: 'plus', color: '#5d5', onClick: 'showTable'}));
aladin.addCatalog(A.catalogFromVizieR('J/ApJ/562/446/table13', 'M 20', 0.2, {shape: 'square', sourceSize: 8, color: 'red', onClick: 'showPopup'}));