// Make one pass over the links so we can look things up fast later
function buildAdjacency(nodes, links) {
  const map = Object.fromEntries(nodes.map((d) => [d.id, []]));
  links.forEach((l) => {
    map[l.source.id || l.source].push(l);
    map[l.target.id || l.target].push(l);
  });
  return map;
}
// Tag “world-hoppers”: a node that has at least one edge to a
// character from a *different* homeworld.
function tagHoppers(nodes, links) {
  const adj = buildAdjacency(nodes, links);
  nodes.forEach((n) => {
    const foreign = new Set(
      adj[n.id]
        .map((l) => {
          const other =
            (l.source.id || l.source) === n.id ? l.target : l.source;
          return other.homeworld || other.homeworld; // robust to raw / id form
        })
        .filter((hw) => hw !== n.homeworld),
    );
    n.hopper = foreign.size > 0;
  });
}

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
  const filteredNodes = data.nodes.filter((d) => d.occurrence > 10);
  const nodeIds = new Set(filteredNodes.map((d) => d.id));
  const filteredLinks = data.links.filter(
    (d) =>
      nodeIds.has(d.source.id || d.source) &&
      nodeIds.has(d.target.id || d.target),
  );

  tagHoppers(filteredNodes, filteredLinks);

  const worlds = Array.from(new Set(filteredNodes.map((d) => d.homeworld)));
  const centreRadius = Math.min(width, height) * 0.33; // distance of cluster centres from canvas centre
  const clusterCentre = {}; // lookup table
  worlds.forEach((w, i) => {
    const angle = (i / worlds.length) * 2 * Math.PI;
    clusterCentre[w] = {
      x: width / 2 + Math.cos(angle) * centreRadius,
      y: height / 2 + Math.sin(angle) * centreRadius,
    };
  });

  const linkScale = d3
    .scaleLinear()
    .domain(d3.extent(filteredLinks, (d) => d.weight))
    .range([0.9, 2]);

  const opacityScale = d3
    .scaleLinear()
    .domain(d3.extent(filteredLinks, (d) => d.weight))
    .range([0.2, 1]);

  const radiusScale = d3
    .scaleSqrt()
    .domain(d3.extent(filteredNodes, (d) => d.occurrence))
    .range([5, 20]);

  const maxOccurrence = d3.max(filteredNodes, (d) => d.occurrence || 1);
  const maxW = d3.max(filteredLinks, (d) => d.weight);
  const distScale = d3
    .scaleLinear()
    .domain([1, maxW]) // small weight → long link
    .range([160, 40]); // tweak to taste

  const simulation = d3
    .forceSimulation(filteredNodes)
    .force(
      "link",
      d3
        .forceLink(filteredLinks)
        .id((d) => d.id)
        .distance((d) => {
          // shorter for heavy links, a bit shorter again if same world
          const base = distScale(d.weight);
          return d.source.homeworld === d.target.homeworld ? base * 0.65 : base;
        })
        .strength((d) => 0.2 + d.weight / maxW),
    )
    .force("charge", d3.forceManyBody().strength(-100))
    .force(
      "collide",
      d3
        .forceCollide()
        .radius((d) => radiusScale(d.occurrence))
        .iterations(2),
    )
    // add two gentle directional forces that pull every node toward the
    // pre-calculated centre of its own homeworld cluster
    .force(
      "xCluster",
      d3.forceX((d) => clusterCentre[d.homeworld].x).strength(0.3),
    )
    .force(
      "yCluster",
      d3.forceY((d) => clusterCentre[d.homeworld].y).strength(0.3),
    )
    // Eventually kill computation
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
    .data(filteredLinks)
    .join("line")
    .attr("stroke-width", (d) => linkScale(d.weight))
    .attr("stroke-opacity", (d) => opacityScale(d.weight))
    .attr("stroke", (d) => {
      const s = d.source.homeworld;
      const t = d.target.homeworld;
      return s === t ? "#999" : "#ffbbb2";
    });

  const node = nodeGroup
    .selectAll("circle")
    .data(filteredNodes)
    .join("circle")
    .attr("r", (d) => radiusScale(d.occurrence))
    .attr("fill", (d) => colorScale(d.homeworld))
    .attr("stroke", (d) => (d.hopper ? "#000" : "#fff")) // thicker outline for hoppers
    .attr("stroke-width", (d) => (d.hopper ? 3 : 1.5));

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

  const legend = svg
    .append("g")
    .attr("class", "legend")
    .attr("transform", `translate(20,20)`);
  legend
    .append("text")
    .text("Homeworld")
    .attr("x", 0)
    .attr("y", -8)
    .style("font-weight", "bold")
    .style("font-size", "14px");
  worlds.forEach((w, i) => {
    const g = legend.append("g").attr("transform", `translate(0,${i * 20})`);
    g.append("rect")
      .attr("width", 14)
      .attr("height", 14)
      .attr("fill", colorScale(w));
    g.append("text")
      .attr("x", 18)
      .attr("y", 12)
      .text(w)
      .style("font-size", "12px");
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
