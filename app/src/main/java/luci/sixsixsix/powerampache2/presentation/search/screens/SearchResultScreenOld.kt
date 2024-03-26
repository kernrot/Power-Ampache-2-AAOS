package luci.sixsixsix.powerampache2.presentation.search.screens

import androidx.activity.compose.BackHandler
import androidx.compose.foundation.gestures.detectTapGestures
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.ExperimentalComposeUiApi
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.platform.LocalFocusManager
import androidx.compose.ui.platform.LocalSoftwareKeyboardController
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.hilt.navigation.compose.hiltViewModel
import com.ramcosta.composedestinations.navigation.DestinationsNavigator
import luci.sixsixsix.powerampache2.presentation.common.LoadingScreen
import luci.sixsixsix.powerampache2.presentation.main.viewmodel.MainEvent
import luci.sixsixsix.powerampache2.presentation.main.viewmodel.MainViewModel
import luci.sixsixsix.powerampache2.presentation.screens.albums.AlbumsScreen
import luci.sixsixsix.powerampache2.presentation.screens.albums.AlbumsViewModel
import luci.sixsixsix.powerampache2.presentation.screens.artists.ArtistsScreen
import luci.sixsixsix.powerampache2.presentation.screens.artists.ArtistsViewModel
import luci.sixsixsix.powerampache2.presentation.screens.playlists.PlaylistsScreen
import luci.sixsixsix.powerampache2.presentation.screens.playlists.PlaylistsViewModel
import luci.sixsixsix.powerampache2.presentation.screens.songs.SongsListScreen
import luci.sixsixsix.powerampache2.presentation.screens.songs.SongsViewModel


@OptIn(ExperimentalComposeUiApi::class)
@Composable
fun SearchResultsScreenOld(
    navigator: DestinationsNavigator,
    modifier: Modifier = Modifier,
    mainViewModel: MainViewModel,
    songsViewModel: SongsViewModel = hiltViewModel(),
    albumsViewModel: AlbumsViewModel = hiltViewModel(),
    artistsViewModel: ArtistsViewModel = hiltViewModel(),
    playlistsViewModel: PlaylistsViewModel = hiltViewModel()
) {
    val controller = LocalSoftwareKeyboardController.current
    val localFocusManager = LocalFocusManager.current

    val songsState = songsViewModel.state
    val albumsState = albumsViewModel.state
    val artistsState = artistsViewModel.state
    val playlistsState = playlistsViewModel.state
    val playlistsStateFlow by playlistsViewModel.playlistsStateFlow.collectAsState()

    BackHandler {
        mainViewModel.onEvent(MainEvent.OnSearchQueryChange(""))
    }

    if (songsState.isLoading &&
        albumsState.isLoading &&
        artistsState.isLoading &&
        playlistsState.isLoading
    ) {
        LoadingScreen()
    }

    Column(modifier = modifier.pointerInput(Unit) {
        detectTapGestures(onTap = {
            localFocusManager.clearFocus()
        })
    }) {
        if (!songsState.isLoading &&
            !albumsState.isLoading &&
            !artistsState.isLoading &&
            !playlistsState.isLoading &&
            songsState.songs.isEmpty() &&
            albumsState.albums.isEmpty() &&
            artistsState.artists.isEmpty() &&
            playlistsStateFlow.isEmpty()
        ) {
            SearchSectionTitleText(text = "Nothing seems to match your search query")
        }

        if (songsState.songs.isNotEmpty()) {
            SearchSectionTitleText(text = "Songs")
            SongsListScreen(
                modifier = Modifier.weight(1f),
                navigator = navigator,
                mainViewModel = mainViewModel,
                viewModel = songsViewModel
            )
        }

        if (albumsState.albums.isNotEmpty()) {
            SearchSectionTitleText(text = "Albums")
            AlbumsScreen(
                modifier = Modifier.weight(1f),
                navigator = navigator,
                viewModel = albumsViewModel,
                gridItemsRow = 4,
                minGridItemsRow = 3
            )
        }

        if (playlistsStateFlow.isNotEmpty()) {
            SearchSectionTitleText(text = "Playlists")
            PlaylistsScreen(
                modifier = Modifier.weight(1f),
                navigator = navigator,
                viewModel = playlistsViewModel,
            )
        }

        if (artistsState.artists.isNotEmpty()) {
            SearchSectionTitleText(text = "Artists")
            ArtistsScreen(
                modifier = Modifier.weight(1f),
                navigator = navigator,
                viewModel = artistsViewModel,
                gridPerRow = 4
            )
        }

        // add spacing below if empty results
        val atLeastOneEmpty = songsState.songs.isEmpty()||albumsState.albums.isEmpty()||
                artistsState.artists.isEmpty() ||playlistsStateFlow.isEmpty()
        val totalItems = songsState.songs.size + albumsState.albums.size +
                artistsState.artists.size + playlistsStateFlow.size
        if (atLeastOneEmpty && totalItems < 5) {
            Box(modifier = Modifier.weight(2.0f))
        } else if (totalItems < 11) {
            Box(modifier = Modifier.weight(1.0f))
        }
    }
}


@Composable
private fun SearchSectionTitleText(text: String, modifier: Modifier = Modifier) {
    Card(
        modifier = modifier.fillMaxWidth(),
        shape = RoundedCornerShape(2.dp),
        colors = CardDefaults.cardColors(
            containerColor = MaterialTheme.colorScheme.secondaryContainer
        )
    ) {
        Box(
            modifier = Modifier.fillMaxWidth(),
            contentAlignment = Alignment.CenterStart
        ) {
            Text(
                modifier = Modifier
                    .padding(horizontal = 11.dp)
                    .padding(top = 5.dp, bottom = 4.dp),
                text = text,
                color = MaterialTheme.colorScheme.onSecondaryContainer,
                fontSize = 17.sp,
                fontWeight = FontWeight.Medium,
            )
        }
    }
}
