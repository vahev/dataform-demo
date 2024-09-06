const covidMxAssertions = {
    nonNull: ["country_code", "country_name"],
    rowConditions: [
        "LENGTH(country_code) <= 3"
    ]
};

module.exports = { 
    covidMxAssertions 
};