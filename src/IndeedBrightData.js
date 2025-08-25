
export default async function IndeedBrightData(keyword, count){

try {
    const data = JSON.stringify([
        {"country":"US","domain":"indeed.com","keyword_search":keyword,"location":"United States","date_posted":'Last 24 Hours' ,"posted_by":"","location_radius":""},
    ]);

    let indeedFetch = await fetch(
        `https://api.brightdata.com/datasets/v3/scrape?dataset_id=gd_l4dx9j9sscpvs7no2&include_errors=true&type=discover_new&discover_by=keyword&limit_per_input=${count}`,
        {
            method: "POST",        
            headers: {
                "Authorization": "Bearer ab23cac898b6dc350bae00969b913356b4c7145a1b96c0bf76b54c4f65978b14",
                "Content-Type": "application/json",
            },
            body: data,
        }
    )
let finalRes = await indeedFetch.json();    
return finalRes;
} catch (error) {
    console.log(error)
}
}