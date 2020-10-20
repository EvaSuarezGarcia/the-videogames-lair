function toggleAdvancedFilters(button) {
    $('#advanced-filters').toggleClass(["collapsed", "overflow-hidden"]);
    $(button).find(".fas").toggleClass("fa-rotate-180");
}