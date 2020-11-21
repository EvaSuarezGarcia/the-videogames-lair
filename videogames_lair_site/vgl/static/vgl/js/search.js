function toggleAdvancedFilters(button) {
    $('#advanced-filters').toggleClass(["collapsed"]);
    $(button).find(".fas").toggleClass("fa-rotate-180");
}

function createFilter(hiddenInput, visibleValue) {
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

    return span;
}

function addFilter(originalInput, visibleValue, hiddenValue) {
    const hiddenInput = originalInput.cloneNode();
    hiddenInput.type = "hidden";
    hiddenInput.value = hiddenValue;

    const span = createFilter(hiddenInput, visibleValue);

    originalInput.parentNode.appendChild(span);
    originalInput.value = "";
}

const ageRatingOptions = document.getElementById("input-age-rating")
    .querySelectorAll("option");

function updateAgeRatings(select) {
    const system = select.value;
    const ageRatingsSelect = document.getElementById("input-age-rating");

    ageRatingsSelect.innerHTML = "";
    for (let i = 0; i < ageRatingOptions.length; i++) {
        const ageRating = ageRatingOptions[i];
        if (!(ageRating.dataset.option) || (ageRating.dataset.option === system)) {
            ageRatingsSelect.appendChild(ageRating);
        }
    }
    document.getElementById("input-age-rating").disabled = (system === "System");
}

function addAgeRatingFilter(select) {
    const ageRating = select.value;
    const systemSelect = document.getElementById("input-age-rating-system");
    const system = systemSelect.value;
    const fullAgeRating = `${system}: ${ageRating}`;

    const hiddenInput = document.createElement("input");
    hiddenInput.type = "hidden";
    hiddenInput.value = fullAgeRating;
    hiddenInput.name = select.name;

    const span = createFilter(hiddenInput, fullAgeRating);

    const container = document.getElementById("age-rating-filters-container");
    container.appendChild(span);
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
