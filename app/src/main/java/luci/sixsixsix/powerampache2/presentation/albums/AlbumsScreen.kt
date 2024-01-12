package luci.sixsixsix.powerampache2.presentation.albums

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.wrapContentHeight
import androidx.compose.foundation.layout.wrapContentSize
import androidx.compose.foundation.lazy.grid.GridCells
import androidx.compose.foundation.lazy.grid.LazyVerticalGrid
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.alpha
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalConfiguration
import androidx.compose.ui.res.dimensionResource
import androidx.compose.ui.unit.dp
import androidx.hilt.navigation.compose.hiltViewModel
import com.google.accompanist.swiperefresh.SwipeRefresh
import com.google.accompanist.swiperefresh.rememberSwipeRefreshState
import com.ramcosta.composedestinations.annotation.Destination
import com.ramcosta.composedestinations.navigation.DestinationsNavigator
import luci.sixsixsix.powerampache2.R
import luci.sixsixsix.powerampache2.domain.models.key
import luci.sixsixsix.powerampache2.presentation.LoadingScreen
import luci.sixsixsix.powerampache2.presentation.albums.components.AlbumItem
import luci.sixsixsix.powerampache2.presentation.destinations.AlbumDetailScreenDestination

const val GRID_ITEMS_ROW = 2
const val GRID_ITEMS_ROW_MIN = 2

@Destination
@Composable
fun AlbumsScreen(
    navigator: DestinationsNavigator,
    modifier: Modifier = Modifier,
    gridItemsRow: Int = GRID_ITEMS_ROW,
    minGridItemsRow: Int = GRID_ITEMS_ROW_MIN,
    viewModel: AlbumsViewModel = hiltViewModel()
) {
    val swipeRefreshState = rememberSwipeRefreshState(isRefreshing = viewModel.state.isRefreshing)
    val state = viewModel.state
    val cardsPerRow = if (state.albums.size < 5) { minGridItemsRow } else { gridItemsRow }
    val albumCardSize = (LocalConfiguration.current.screenWidthDp / cardsPerRow).dp

    if (state.isLoading && state.albums.isEmpty()) {
        LoadingScreen()
    }

    Box(
        modifier = modifier
    ) {
        SwipeRefresh(
            state = swipeRefreshState,
            onRefresh = { viewModel.onEvent(AlbumsEvent.Refresh) }
        ) {
            LazyVerticalGrid(
                columns = GridCells.Fixed(cardsPerRow)
            ) {
                items(
                    count = state.albums.size,
                    key = { i -> state.albums[i].key() })
                { i ->
                    val album = state.albums[i]
                    AlbumItem(
                        album = album,
                        modifier = Modifier
                            .size(albumCardSize)
                            .clickable {
                                navigator.navigate(AlbumDetailScreenDestination(album.id, album))
                            }
                            .padding(
                                horizontal = dimensionResource(id = R.dimen.albumItem_paddingHorizontal),
                                vertical = dimensionResource(id = R.dimen.albumItem_paddingVertical)
                            )
                    )
                    // search queries are limited, do not fetch more in case of a search string
                    if (i == (state.albums.size - 1) && state.searchQuery.isBlank()) {
                        // if last item, fetch more
                        viewModel.onEvent(AlbumsEvent.OnBottomListReached(i))
                    }
                }
            }
        }

        if (state.isFetchingMore) {
            LoadingView(modifier = Modifier.align(Alignment.BottomCenter))
        }
    }
}

@Composable
fun LoadingView(modifier: Modifier = Modifier) {
    Card(
        //colors = CardDefaults.cardColors(containerColor = Color.Transparent),
        modifier = modifier
            .wrapContentSize()
    ) {
        Box(modifier = Modifier.wrapContentSize()) {
            CircularProgressIndicator(modifier = Modifier.size(22.dp).align(Alignment.Center))
        }
    }
}
