const width = window.innerWidth;
const height = window.innerHeight;

const svg = d3
  .select("#main_canvas")
  .attr("viewBox", [0, 0, width, height])
  .attr("preserveAspectRatio", "xMidYMid meet");

const container = svg.append("g");
const linkGroup = container.append("g").attr("stroke", "#999");
const nodeGroup = container
  .append("g")
  .attr("stroke", "#fff")
  .attr("stroke-width", 1.5);

const zoom = d3
  .zoom()
  .scaleExtent([0.5, 5]) // Adjust the scale limits as needed
  .on("zoom", (event) => {
    container.attr("transform", event.transform);
  });

svg.call(zoom);

d3.json("temp.json").then((data) => {
  const linkScale = d3
    .scaleLinear()
    .domain(d3.extent(data.links, (d) => d.weight))
    .range([0.9, 2]);

  const opacityScale = d3
    .scaleLinear()
    .domain(d3.extent(data.links, (d) => d.weight))
    .range([0.9, 1]);

  const radiusScale = d3
    .scaleSqrt()
    .domain(d3.extent(data.nodes, (d) => d.occurrence))
    .range([5, 20]);

  const maxOccurrence = d3.max(data.nodes, (d) => d.occurrence || 1);

  const simulation = d3
    .forceSimulation(data.nodes)
    .force(
      "link",
      d3
        .forceLink(data.links)
        .id((d) => d.id)
        .distance((d) => {
          const sourceOccurrence = d.source.occurrence || 1;
          const targetOccurrence = d.target.occurrence || 1;
          const avgOccurrence = (sourceOccurrence + targetOccurrence) / 2;
          const occurrenceFactor = avgOccurrence / maxOccurrence;

          const homeworldFactor =
            d.source.homeworld === d.target.homeworld ? 0.5 : 1;

          return 1 * occurrenceFactor * homeworldFactor;
        })
        .strength(0.1),
    )
    .force("charge", d3.forceManyBody().strength(-100))
    .force(
      "collide",
      d3
        .forceCollide()
        .radius((d) => radiusScale(d.occurrence))
        .iterations(2),
    )
    .on("end", () => {
      simulation.stop();
    });

  // Global positing
  // d3.forceCenter(width / 2, height / 2);
  // d3.forceX(width / 2).strength(0.5);
  // d3.forceY(height / 2).strength(0.5);
  // d3.forceManyBody().strength(-300).distanceMax(3000);

  const colorScale = d3.scaleOrdinal(d3.schemeCategory10);

  // Create links with data binding
  const link = linkGroup
    .selectAll("line")
    .data(data.links)
    .join("line")
    .attr("stroke-width", (d) => linkScale(d.weight))
    .attr("stroke-opacity", (d) => opacityScale(d.weight));

  // Create nodes with data binding
  // TODO: isolate worldhoppers and color them differently
  const node = nodeGroup
    .selectAll("circle")
    .data(data.nodes)
    .join("circle")
    .attr("r", (d) => radiusScale(d.occurrence))
    .attr("fill", (d) => colorScale(d.homeworld));

  // Add tooltips to nodes
  node.append("title").text((d) => d.name);

  // Simulation tick
  simulation.on("tick", () => {
    link
      .attr("x1", (d) => d.source.x)
      .attr("y1", (d) => d.source.y)
      .attr("x2", (d) => d.target.x)
      .attr("y2", (d) => d.target.y);

    node.attr("cx", (d) => d.x).attr("cy", (d) => d.y);
  });

  const tooltip = d3
    .select("body")
    .append("div")
    .attr("class", "tooltip")
    .style("position", "absolute")
    .style("background", "#fff")
    .style("padding", "5px")
    .style("border", "1px solid #ccc")
    .style("border-radius", "3px")
    .style("visibility", "hidden");

  node
    .on("mouseover", function (event, d) {
      tooltip.text(d.name);
      tooltip.style("visibility", "visible");
    })
    .on("mousemove", function (event) {
      tooltip
        .style("top", event.pageY - 10 + "px")
        .style("left", event.pageX + 10 + "px");
    })
    .on("mouseout", function () {
      tooltip.style("visibility", "hidden");
    });

  node.on("click", function (event, d) {
    const infoPanel = d3.select("#node-info");
    const details = d3.select("#node-details");
    details.html("");

    for (const [key, value] of Object.entries(d)) {
      details.append("div").html(`<strong>${key}:</strong> ${value}`);
    }

    infoPanel.style("display", "block");
  });
  d3.select("#close-panel").on("click", function () {
    d3.select("#node-info").style("display", "none");
  });
});
