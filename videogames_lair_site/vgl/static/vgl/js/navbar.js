/*
 * Initialize overlay (used in mobile only).
 * Requires jQuery.
 */

$(document).ready(function () {
    $("#navbarMenuToggler, #dismiss, .overlay").on("click", function () {
        $(".overlay").toggleClass("overlay-active");
    });
});
