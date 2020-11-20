function toggleAdvancedFilters(button) {
    $('#advanced-filters').toggleClass(["collapsed"]);
    $(button).find(".fas").toggleClass("fa-rotate-180");
}

function addFilter(original_input, visible_value, hidden_value) {
    let hidden_input = original_input.cloneNode();
    hidden_input.type = "hidden";
    hidden_input.value = hidden_value;

    let span = document.createElement("span");
    span.innerText = visible_value;
    span.classList.add("bg-info", "text-white", "rounded-pill", "px-2", "mt-1", "mr-1", "d-inline-block");
    span.appendChild(hidden_input);

    let close = document.createElement("button");
    close.type = "button";
    close.classList.add("close", "text-white", "ml-1", "line-height-inherit", "font-weight-inherit",
        "font-size-inherit");
    close.setAttribute("aria-label", "Close");
    let close_span = document.createElement("span");
    close_span.innerHTML = "&times;"
    close_span.setAttribute("aria-hidden", "true");
    close.appendChild(close_span);
    span.appendChild(close);

    original_input.parentNode.insertBefore(span, original_input.parentNode.lastChild.nextSibling);
    original_input.value = "";
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
            let split_value = value.split(" ");
            let platform_abbreviation = split_value[0];
            let platform_name = split_value.slice(1).join(" ");
            platform_name = platform_name.slice(1, platform_name.length-1); // Remove parenthesis

            addFilter(this, platform_abbreviation, platform_name);
    });
});