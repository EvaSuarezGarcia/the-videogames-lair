function toggleAdvancedFilters(button) {
    $('#advanced-filters').toggleClass(["collapsed"]);
    $(button).find(".fas").toggleClass("fa-rotate-180");
}

function addFilter(originalInput, visibleValue, hiddenValue) {
    const hiddenInput = originalInput.cloneNode();
    hiddenInput.type = "hidden";
    hiddenInput.value = hiddenValue;

    const span = document.createElement("span");
    span.innerText = visibleValue;
    span.classList.add("bg-info", "text-white", "rounded-pill", "px-2", "mt-1", "mr-1", "d-inline-block");
    span.appendChild(hiddenInput);

    const close = document.createElement("button");
    close.type = "button";
    close.classList.add("close", "text-white", "ml-1", "line-height-inherit", "font-weight-inherit",
        "font-size-inherit");
    close.setAttribute("aria-label", "Close");
    const closeSpan = document.createElement("span");
    closeSpan.innerHTML = "&times;"
    closeSpan.setAttribute("aria-hidden", "true");
    close.appendChild(closeSpan);
    span.appendChild(close);

    originalInput.parentNode.insertBefore(span, originalInput.parentNode.lastChild.nextSibling);
    originalInput.value = "";
}

$(document).ready(function () {
    $("form").submit(function () {
        $(".advanced-search:not([type=hidden])").remove();
    });

    $(".js-range-slider").ionRangeSlider({
        type: "double",
        skin: "round",
        min: 1971,
        max: 2020,
        prettify: (year) => year
    });

    $(".basic-autocomplete").autoComplete({minLength: 1, preventEnter: true});

    $(".basic-autocomplete:not(#platform-input)").on("autocomplete.select", function (event, value) {
            addFilter(this, value, value);
    });

    $("#platform-input").on("autocomplete.select", function (event, value) {
            const splitValue = value.split(" ");
            const platformAbbreviation = splitValue[0];
            let platformName = splitValue.slice(1).join(" ");
            platformName = platformName.slice(1, platformName.length-1); // Remove parenthesis

            addFilter(this, platformAbbreviation, platformName);
    });
});
